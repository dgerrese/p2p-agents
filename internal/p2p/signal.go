package p2p

import (
	"context"
	"sync"

	"github.com/nbd-wtf/go-nostr"
)

// Signal carries sealed, opaque envelope events between peers, routed by p tag.
type Signal interface {
	Publish(ctx context.Context, ev nostr.Event) error
	// Subscribe replaces the whole set of mailbox pubkeys listened for.
	Subscribe(mailboxPubs []string)
	Events() <-chan nostr.Event
	Close() error
}

// inbox is the shared receive side: subscription filter, dedup, one output channel.
type inbox struct {
	mu     sync.Mutex
	subs   map[string]bool
	seen   map[string]bool
	out    chan nostr.Event
	closed bool
}

func newInbox() *inbox {
	return &inbox{subs: map[string]bool{}, seen: map[string]bool{}, out: make(chan nostr.Event, 256)}
}

func (in *inbox) setSubs(pubs []string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.subs = map[string]bool{}
	for _, p := range pubs {
		in.subs[p] = true
	}
}

// markSeen records id and reports whether it was already seen.
func (in *inbox) markSeen(id string) bool {
	if len(in.seen) > 10000 { // ponytail: wholesale reset lets an old dup through, use an LRU if that matters
		in.seen = map[string]bool{}
	}
	dup := in.seen[id]
	in.seen[id] = true
	return dup
}

// deliver passes ev on if its p tag is subscribed and it is new. Drops when the consumer lags.
func (in *inbox) deliver(ev nostr.Event) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.closed {
		return
	}
	tag := ev.Tags.Find("p")
	if len(tag) < 2 || !in.subs[tag[1]] || in.markSeen(ev.ID) {
		return
	}
	select {
	case in.out <- ev:
	default: // ponytail: drop on a full 256 buffer, signaling is retried by the protocol
	}
}

func (in *inbox) close() {
	in.mu.Lock()
	defer in.mu.Unlock()
	if !in.closed {
		in.closed = true
		close(in.out)
	}
}

// MemBus is an in-process signaling bus for tests.
type MemBus struct {
	mu      sync.Mutex
	members []*memSignal
}

func NewMemBus() *MemBus { return &MemBus{} }

func (b *MemBus) Join() Signal {
	m := &memSignal{bus: b, inbox: newInbox()}
	b.mu.Lock()
	b.members = append(b.members, m)
	b.mu.Unlock()
	return m
}

type memSignal struct {
	*inbox
	bus *MemBus
}

func (m *memSignal) Publish(_ context.Context, ev nostr.Event) error {
	m.bus.mu.Lock()
	members := append([]*memSignal(nil), m.bus.members...)
	m.bus.mu.Unlock()
	for _, o := range members {
		if o != m {
			o.deliver(ev)
		}
	}
	return nil
}

func (m *memSignal) Subscribe(pubs []string)    { m.setSubs(pubs) }
func (m *memSignal) Events() <-chan nostr.Event { return m.out }
func (m *memSignal) Close() error               { m.close(); return nil }
