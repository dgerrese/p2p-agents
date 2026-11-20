package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// Event is what the Node reports to the MCP/CLI layer. It never carries a verify code.
type Event struct {
	Kind   string `json:"kind"` // message|pair_request|paired|pair_failed|peer_online|peer_offline|unpaired
	Peer   string `json:"peer,omitempty"`
	Name   string `json:"name,omitempty"`
	Text   string `json:"text,omitempty"`
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// box is a mailbox the node listens on.
type box struct {
	sk   string
	peer string // paired peer pub; "" for pairing mailboxes
}

type Node struct {
	st      *Store
	sk, pub string
	name    string
	sigs    []Signal // LAN first when both are set
	stun    []string
	events  chan Event

	connected atomic.Int32 // PeerConnections that reached connected; for tests

	mu      sync.Mutex
	closed  bool
	invites map[string]*invite  // by one-time mailbox pub
	reqs    map[string]*pairReq // by request ID
	boxes   map[string]box      // subscribed mailbox pub -> key
	window  int64
	seen    map[string]bool // inner event IDs, only touched by the Run goroutine
	known   map[string]Peer // paired peers by pub, refreshed on resubscribe
	conns   map[string]*peerConn
	dials   map[string]*dialState
	acks    map[string]chan string // message ID -> ack status
}

var newRelays = NewRelays // seam for TestLANOnlyNoRelays

// NewNodeFromConfig builds the signals from config: LAN unless P2PA_NO_LAN=1, relays unless lan_only.
func NewNodeFromConfig(st *Store) (*Node, error) {
	cfg, err := st.Config()
	if err != nil {
		return nil, err
	}
	var lan, relay Signal
	if os.Getenv("P2PA_NO_LAN") != "1" {
		if lan, err = NewLAN(); err != nil {
			log.Printf("LAN signaling unavailable: %v", err)
			lan = nil
		}
	}
	if !cfg.LANOnly {
		relay = newRelays(cfg.Relays)
	}
	return NewNode(st, lan, relay)
}

// NewNode takes ownership of the signals; either may be nil, not both.
func NewNode(st *Store, lan, relay Signal) (*Node, error) {
	var sigs []Signal
	for _, s := range []Signal{lan, relay} {
		if s != nil {
			if debugOn() {
				s = debugWrap(s)
			}
			sigs = append(sigs, s)
		}
	}
	if len(sigs) == 0 {
		return nil, errors.New("no signal transport")
	}
	sk, pub, err := st.Identity()
	if err != nil {
		return nil, err
	}
	cfg, err := st.Config()
	if err != nil {
		return nil, err
	}
	n := &Node{
		st: st, sk: sk, pub: pub, name: cfg.Name, sigs: sigs, stun: cfg.STUN,
		events:  make(chan Event, 256),
		invites: map[string]*invite{}, reqs: map[string]*pairReq{}, seen: map[string]bool{},
		conns: map[string]*peerConn{}, dials: map[string]*dialState{}, acks: map[string]chan string{},
	}
	if cfg.LANOnly { // STUN would disclose the public IP; host candidates suffice on a LAN
		n.stun = nil
	}
	return n, n.resubscribe()
}

func (n *Node) Events() <-chan Event { return n.events }

// Run processes incoming envelopes until ctx is done, then closes the signals and Events.
func (n *Node) Run(ctx context.Context) error {
	in := make(chan nostr.Event)
	for _, s := range n.sigs {
		go func() {
			for ev := range s.Events() {
				select {
				case in <- ev:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	defer func() {
		for _, s := range n.sigs {
			s.Close()
		}
		n.mu.Lock()
		n.closed = true
		close(n.events)
		conns := n.conns
		n.conns = map[string]*peerConn{}
		n.mu.Unlock()
		for _, c := range conns {
			c.pc.Close()
		}
	}()
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	dial := time.NewTicker(dialTick)
	defer dial.Stop()
	n.maintain()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-in:
			n.handle(ev)
		case <-tick.C:
			n.mu.Lock()
			if n.pruneLocked() || Window(now()) != n.window {
				n.resubscribeLocked()
			}
			n.mu.Unlock()
		case <-dial.C:
			n.maintain()
		}
	}
}

func (n *Node) handle(ev nostr.Event) {
	tag := ev.Tags.Find("p")
	if len(tag) < 2 {
		return
	}
	n.mu.Lock()
	b, ok := n.boxes[tag[1]]
	n.mu.Unlock()
	if !ok {
		return
	}
	p, sender, innerID, err := Open(ev, b.sk)
	if err != nil || sender == n.pub || n.markSeen(innerID) {
		return
	}
	switch p.Type {
	case "pair_req", "pair_resp", "pair_reveal", "pair_ok", "pair_no":
		if b.peer == "" {
			n.handlePair(tag[1], p, sender)
		}
	case "knock", "offer", "answer", "bye":
		if b.peer != "" && sender == b.peer { // only the peer owning this mailbox
			n.handleConn(b.peer, p)
		}
	}
}

// markSeen records an inner event ID and reports whether it was seen before.
func (n *Node) markSeen(id string) bool {
	if len(n.seen) > 10000 { // ponytail: wholesale reset lets an old replay through within maxSkew, use an LRU if that matters
		n.seen = map[string]bool{}
	}
	dup := n.seen[id]
	n.seen[id] = true
	return dup
}

func (n *Node) resubscribe() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.resubscribeLocked()
}

// pruneLocked drops expired invites and requests (emitting pair_failed) and reports whether any went.
func (n *Node) pruneLocked() bool {
	t, pruned := now(), false
	for pub, inv := range n.invites {
		if t.After(inv.expires) {
			delete(n.invites, pub)
			pruned = true
		}
	}
	for id, r := range n.reqs {
		if !t.Before(r.Expires) {
			delete(n.reqs, id)
			n.emitLocked(Event{Kind: "pair_failed", Peer: r.PeerPub, Name: r.PeerName, ID: id, Reason: "expired"})
			pruned = true
		}
	}
	return pruned
}

// resubscribeLocked prunes expired state and recomputes the mailbox set:
// pending invites, pending pairing requests, and each peer's windows w-1..w+1.
func (n *Node) resubscribeLocked() error {
	peers, err := n.st.Peers()
	if err != nil {
		return err
	}
	n.pruneLocked()
	n.window = Window(now())
	boxes, known := map[string]box{}, map[string]Peer{}
	for pub, inv := range n.invites {
		boxes[pub] = box{sk: inv.sk}
	}
	for _, r := range n.reqs {
		boxes[r.listenPub] = box{sk: r.listenSK}
	}
	for _, p := range peers {
		if len(p.Secret) == 0 { // MailboxKey panics on it; a broken entry must not kill the node
			continue
		}
		known[p.Pub] = p
		for w := n.window - 1; w <= n.window+1; w++ {
			sk, pub := MailboxKey(p.Secret, w)
			boxes[pub] = box{sk: sk, peer: p.Pub}
		}
	}
	n.boxes, n.known = boxes, known
	pubs := slices.Collect(maps.Keys(boxes))
	for _, s := range n.sigs {
		s.Subscribe(pubs)
	}
	return nil
}

// emitLocked never blocks; callers hold n.mu.
func (n *Node) emitLocked(e Event) {
	if n.closed {
		return
	}
	select {
	case n.events <- e:
	default: // ponytail: drops when 256 events go unread, block or grow if consumers lag in practice
	}
}

// seal wraps a payload for mailbox and logs a failure; callers publish the result after releasing n.mu.
func (n *Node) seal(typ string, data any, mailbox string) *nostr.Event {
	ev, err := sealPayload(n.sk, typ, data, mailbox)
	if err != nil {
		log.Printf("p2p-agents: cannot seal %s: %v", typ, err)
	}
	return ev
}

func sealPayload(sk, typ string, data any, mailbox string) (*nostr.Event, error) {
	d, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	ev, err := Seal(Payload{Type: typ, Data: d}, sk, mailbox)
	if err != nil {
		return nil, err
	}
	return &ev, nil
}

// publish sends ev on each signal in turn (LAN, then relays) without waiting for a reply, and succeeds if any did.
// ponytail: pairing, knock and bye skip the LAN-first wait (offers and answers use publishFirst), so relays see them even on a shared LAN; route them through publishFirst if that matters.
func (n *Node) publish(ev *nostr.Event) error {
	if ev == nil {
		return errors.New("seal failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	sent := false
	for _, s := range n.sigs {
		if e := s.Publish(ctx, *ev); e != nil {
			err = e
		} else {
			sent = true
		}
	}
	if sent {
		return nil
	}
	return err
}
