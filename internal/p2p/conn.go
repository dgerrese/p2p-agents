package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v4"
)

// Timings are vars so tests could shorten them.
var (
	dialTick      = 500 * time.Millisecond
	offerBase     = 5 * time.Second
	offerMax      = 5 * time.Minute
	knockEvery    = 30 * time.Second
	knockGrace    = 3 * time.Second // a knock this soon after our offer most likely crossed it
	lanWait       = 2 * time.Second
	gatherTimeout = 10 * time.Second
)

var rtcAPI = newRTCAPI(nil)

// newRTCAPI silences pion logging; tune lets tests restrict candidates.
func newRTCAPI(tune func(*webrtc.SettingEngine)) *webrtc.API {
	var se webrtc.SettingEngine
	se.LoggerFactory = &logging.DefaultLoggerFactory{Writer: io.Discard, DefaultLogLevel: logging.LogLevelDisabled}
	se.SetSCTPMaxMessageSize(512 << 10) // bounds what a peer can make us buffer; 64 KB text JSON-escaped fits
	if tune != nil {
		tune(&se)
	}
	return webrtc.NewAPI(webrtc.WithSettingEngine(se))
}

// peerConn is the one PeerConnection kept per peer.
type peerConn struct {
	pc        *webrtc.PeerConnection
	sid       string // pairs an answer with its offer
	created   time.Time
	dc        *webrtc.DataChannel // set once the channel is open: the peer is online
	answered  chan struct{}       // offerer: closed when the answer arrives
	opened    chan struct{}       // closed when dc opens
	gotAnswer bool
}

type dialState struct {
	next    time.Time
	backoff time.Duration
}

type sdpMsg struct {
	SID string `json:"sid"`
	SDP string `json:"sdp"`
}

type byeMsg struct {
	Reason string `json:"reason"`
}

// maintain runs on the Run goroutine: offerer offers with backoff, the other side knocks, while offline.
func (n *Node) maintain() {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := now()
	for pub := range n.known {
		if c := n.conns[pub]; c != nil && c.dc != nil {
			continue
		}
		d := n.dials[pub]
		if d == nil { // new peer or start: due now
			d = &dialState{backoff: offerBase}
			n.dials[pub] = d
		}
		if t.Before(d.next) {
			continue
		}
		if n.pub < pub {
			d.next = t.Add(d.backoff)
			d.backoff = min(2*d.backoff, offerMax)
			go n.offer(pub)
		} else {
			d.next = t.Add(knockEvery)
			ev := n.sealToLocked(pub, "knock", nil)
			go n.publish(ev)
		}
	}
}

// handleConn runs on the Run goroutine for envelopes from peer on its own mailbox; no network I/O here.
func (n *Node) handleConn(peer string, p Payload) {
	offerer := n.pub < peer
	switch p.Type {
	case "knock":
		if !offerer {
			return
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		d, c := n.dials[peer], n.conns[peer]
		switch {
		case d == nil: // maintain dials it on its next tick anyway
		case c != nil && c.dc != nil: // online; a stale channel is caught by ICE disconnect
		case c != nil && now().Sub(c.created) < knockGrace:
			d.backoff = offerBase
			if at := c.created.Add(offerBase); at.Before(d.next) {
				d.next = at
			}
		default:
			d.next, d.backoff = time.Time{}, offerBase
		}
	case "offer":
		var m sdpMsg
		if !offerer && json.Unmarshal(p.Data, &m) == nil {
			go n.answer(peer, m)
		}
	case "answer":
		var m sdpMsg
		if offerer && json.Unmarshal(p.Data, &m) == nil {
			go n.applyAnswer(peer, m)
		}
	case "bye":
		var m byeMsg
		json.Unmarshal(p.Data, &m)
		n.removePeer(peer, cleanText(m.Reason, maxReasonRunes))
	}
}

// newConn creates a PeerConnection for peer and installs it, replacing (and closing) any older one.
func (n *Node) newConn(peer, sid string) *peerConn {
	var cfg webrtc.Configuration
	if len(n.stun) > 0 {
		cfg.ICEServers = []webrtc.ICEServer{{URLs: n.stun}}
	}
	pc, err := rtcAPI.NewPeerConnection(cfg)
	if err != nil {
		return nil
	}
	c := &peerConn{pc: pc, sid: sid, created: now(), answered: make(chan struct{}), opened: make(chan struct{})}
	// Only the answerer accepts one remote channel, the offerer's "p2pa"; every other one is
	// closed before it opens, so a peer cannot make us run a read loop per stream.
	var took atomic.Bool
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if n.pub > peer && dc.Label() == "p2pa" && took.CompareAndSwap(false, true) {
			n.watchDC(peer, c, dc)
			return
		}
		dc.Close()
	})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		switch s {
		case webrtc.PeerConnectionStateConnected:
			n.connected.Add(1)
		case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			n.dropConn(peer, c)
		}
	})
	n.mu.Lock()
	if _, ok := n.known[peer]; n.closed || !ok {
		n.mu.Unlock()
		pc.Close()
		return nil
	}
	old := n.conns[peer]
	n.conns[peer] = c
	if old != nil && old.dc != nil {
		n.emitLocked(Event{Kind: "peer_offline", Peer: peer, Name: n.known[peer].Name})
	}
	n.mu.Unlock()
	if old != nil {
		go old.pc.Close()
	}
	return c
}

// dropConn forgets c if it is still peer's current connection, and closes it.
func (n *Node) dropConn(peer string, c *peerConn) {
	n.mu.Lock()
	if n.conns[peer] == c {
		delete(n.conns, peer)
		if c.dc != nil {
			n.emitLocked(Event{Kind: "peer_offline", Peer: peer, Name: n.known[peer].Name})
		}
	}
	n.mu.Unlock()
	go c.pc.Close() // not from inside a pion callback
}

// gather sets the local description and waits for all ICE candidates (no trickle).
func (c *peerConn) gather(desc webrtc.SessionDescription, err error) (string, error) {
	if err != nil {
		return "", err
	}
	done := webrtc.GatheringCompletePromise(c.pc)
	if err := c.pc.SetLocalDescription(desc); err != nil {
		return "", err
	}
	select {
	case <-done:
	case <-time.After(gatherTimeout): // send what we have
	}
	return c.pc.LocalDescription().SDP, nil
}

func (n *Node) offer(peer string) {
	c := n.newConn(peer, hex.EncodeToString(randBytes(8)))
	if c == nil {
		return
	}
	dc, err := c.pc.CreateDataChannel("p2pa", nil) // ordered by default
	if err != nil {
		n.dropConn(peer, c)
		return
	}
	n.watchDC(peer, c, dc)
	sdp, err := c.gather(c.pc.CreateOffer(nil))
	if err != nil {
		n.dropConn(peer, c)
		return
	}
	n.mu.Lock()
	if n.conns[peer] != c { // superseded while gathering
		n.mu.Unlock()
		return
	}
	ev := n.sealToLocked(peer, "offer", sdpMsg{SID: c.sid, SDP: sdp})
	n.mu.Unlock()
	n.publishFirst(ev, c.answered)
}

func (n *Node) answer(peer string, m sdpMsg) {
	c := n.newConn(peer, m.SID)
	if c == nil {
		return
	}
	if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: m.SDP}); err != nil {
		n.dropConn(peer, c)
		return
	}
	sdp, err := c.gather(c.pc.CreateAnswer(nil))
	if err != nil {
		n.dropConn(peer, c)
		return
	}
	n.mu.Lock()
	if n.conns[peer] != c { // superseded while gathering
		n.mu.Unlock()
		return
	}
	ev := n.sealToLocked(peer, "answer", sdpMsg{SID: m.SID, SDP: sdp})
	n.mu.Unlock()
	n.publishFirst(ev, c.opened)
}

func (n *Node) applyAnswer(peer string, m sdpMsg) {
	n.mu.Lock()
	c := n.conns[peer]
	if c == nil || c.sid != m.SID || c.gotAnswer {
		n.mu.Unlock()
		return
	}
	c.gotAnswer = true
	close(c.answered)
	n.mu.Unlock()
	if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: m.SDP}); err != nil {
		n.dropConn(peer, c)
	}
}

func (n *Node) watchDC(peer string, c *peerConn, dc *webrtc.DataChannel) {
	dc.OnOpen(func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.conns[peer] != c || c.dc != nil {
			return
		}
		c.dc = dc
		close(c.opened)
		if d := n.dials[peer]; d != nil { // reconnect right away if it drops
			d.next, d.backoff = time.Time{}, offerBase
		}
		n.emitLocked(Event{Kind: "peer_online", Peer: peer, Name: n.known[peer].Name})
	})
	dc.OnClose(func() { n.dropConn(peer, c) })
	dc.OnMessage(func(m webrtc.DataChannelMessage) { n.onWire(peer, dc, m.Data) })
}

// sealToLocked seals for peer's current-window mailbox; nil if peer is unknown.
func (n *Node) sealToLocked(peer, typ string, data any) *nostr.Event {
	p, ok := n.known[peer]
	if !ok {
		return nil
	}
	_, mb := MailboxKey(p.Secret, Window(now()))
	return n.seal(typ, data, mb)
}

// publishFirst sends ev on LAN and, unless done closes within lanWait, on relays too.
func (n *Node) publishFirst(ev *nostr.Event, done <-chan struct{}) {
	if ev == nil || len(n.sigs) == 1 {
		n.publish(ev)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if n.sigs[0].Publish(ctx, *ev) == nil {
		select {
		case <-done:
			return
		case <-time.After(lanWait):
		}
	}
	n.sigs[1].Publish(ctx, *ev)
}
