package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/pion/webrtc/v4"
)

const (
	maxText        = 64 << 10
	maxReasonRunes = 500
)

var (
	ackTimeout     = 10 * time.Second
	errUnknownPeer = errors.New("unknown peer")
)

type PeerStatus struct {
	Pub, Name, Fingerprint string
	Online                 bool
}

// wireMsg is one JSON message on the data channel.
type wireMsg struct {
	ID      string    `json:"id"`
	Type    string    `json:"type"` // msg|ack|bye
	Text    string    `json:"text,omitempty"`
	ReplyTo string    `json:"reply_to,omitempty"`
	TS      time.Time `json:"ts"`
	Status  string    `json:"status,omitempty"` // ack: "failed" when rejected, else delivered
}

func sendWire(dc *webrtc.DataChannel, m wireMsg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return dc.SendText(string(b))
}

// isMsgID reports whether s looks like our message IDs: 32 lowercase hex chars.
func isMsgID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

// fingerprint is the first 16 hex chars of pub in groups of 4.
func fingerprint(pub string) string {
	if len(pub) < 16 {
		return pub
	}
	return pub[0:4] + " " + pub[4:8] + " " + pub[8:12] + " " + pub[12:16]
}

func (n *Node) Peers() []PeerStatus {
	ps, err := n.st.Peers()
	if err != nil {
		return nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]PeerStatus, 0, len(ps))
	for _, p := range ps {
		c := n.conns[p.Pub]
		out = append(out, PeerStatus{Pub: p.Pub, Name: p.Name, Fingerprint: fingerprint(p.Pub), Online: c != nil && c.dc != nil})
	}
	return out
}

// Send delivers text over peer's open channel and waits up to ackTimeout for its ack.
// Without an open channel it returns "offline" at once.
func (n *Node) Send(ctx context.Context, peerPub, text, replyTo string) (id, status string, err error) {
	if len(text) > maxText {
		return "", "", fmt.Errorf("message too large: %d bytes, max %d", len(text), maxText)
	}
	n.mu.Lock()
	_, ok := n.known[peerPub]
	c := n.conns[peerPub]
	if !ok || c == nil || c.dc == nil {
		n.mu.Unlock()
		if !ok {
			return "", "", errUnknownPeer
		}
		return "", "offline", nil
	}
	id = hex.EncodeToString(randBytes(16))
	ack := make(chan string, 1)
	n.acks[id] = ack
	dc := c.dc
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.acks, id)
		n.mu.Unlock()
	}()
	m := wireMsg{ID: id, Type: "msg", Text: text, ReplyTo: replyTo, TS: now()}
	if sendWire(dc, m) != nil {
		return id, "offline", nil // channel closed under us
	}
	select {
	case status = <-ack:
	case <-time.After(ackTimeout):
		status = "failed"
	case <-ctx.Done():
		return id, "failed", ctx.Err()
	}
	if status == "delivered" {
		if err := n.st.AppendLog(LogEntry{ID: id, Peer: peerPub, Dir: "out", Text: text, ReplyTo: replyTo, TS: m.TS}); err != nil {
			log.Printf("log write failed: %v", err)
		}
	}
	return id, status, nil
}

// onWire handles one data channel message from peer.
func (n *Node) onWire(peer string, dc *webrtc.DataChannel, data []byte) {
	var m wireMsg
	if json.Unmarshal(data, &m) != nil {
		return
	}
	n.mu.Lock()
	p, ok := n.known[peer]
	n.mu.Unlock()
	if !ok {
		return
	}
	switch m.Type {
	case "msg":
		if len(m.Text) > maxText || !isMsgID(m.ID) || (m.ReplyTo != "" && !isMsgID(m.ReplyTo)) {
			sendWire(dc, wireMsg{ID: m.ID, Type: "ack", Status: "failed", TS: now()})
			return
		}
		if err := n.st.AppendLog(LogEntry{ID: m.ID, Peer: peer, Dir: "in", Text: m.Text, ReplyTo: m.ReplyTo, TS: now()}); err != nil {
			log.Printf("log write failed: %v", err)
		}
		n.mu.Lock()
		n.emitLocked(Event{Kind: "message", Peer: peer, Name: p.Name, Text: m.Text, ID: m.ID})
		n.mu.Unlock()
		sendWire(dc, wireMsg{ID: m.ID, Type: "ack", TS: now()})
	case "ack":
		status := "delivered"
		if m.Status == "failed" {
			status = "failed"
		}
		n.mu.Lock()
		if ch := n.acks[m.ID]; ch != nil {
			select {
			case ch <- status:
			default:
			}
		}
		n.mu.Unlock()
	case "bye":
		n.removePeer(peer, cleanText(m.Text, maxReasonRunes))
	}
}

// Unpair tells the peer (on the channel, or by envelope when offline), then forgets it.
func (n *Node) Unpair(peerPub, reason string) error {
	n.mu.Lock()
	_, ok := n.known[peerPub]
	var dc *webrtc.DataChannel
	if c := n.conns[peerPub]; c != nil {
		dc = c.dc
	}
	ev := n.sealToLocked(peerPub, "bye", byeMsg{Reason: reason})
	n.mu.Unlock()
	if !ok {
		return errUnknownPeer
	}
	if dc != nil && sendWire(dc, wireMsg{Type: "bye", Text: reason, TS: now()}) == nil {
		for i := 0; i < 100 && dc.BufferedAmount() > 0; i++ { // let the bye leave before we close
			time.Sleep(10 * time.Millisecond)
		}
	}
	n.publish(ev) // also by envelope in case the wire bye is lost; the peer drops the second one
	return n.removePeer(peerPub, reason)
}

// removePeer forgets peer: store, connection, dialing, subscriptions; emits unpaired.
func (n *Node) removePeer(peer, reason string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.known[peer]; !ok { // already removed, e.g. our Unpair raced the peer's bye
		return nil
	}
	if err := n.st.RemovePeer(peer); err != nil {
		return err
	}
	if c := n.conns[peer]; c != nil {
		delete(n.conns, peer)
		go c.pc.Close()
	}
	delete(n.dials, peer)
	n.emitLocked(Event{Kind: "unpaired", Peer: peer, Name: n.known[peer].Name, Reason: reason})
	return n.resubscribeLocked()
}
