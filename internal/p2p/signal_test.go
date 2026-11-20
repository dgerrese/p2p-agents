package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func testEvent(t *testing.T, mailbox, content string) nostr.Event {
	t.Helper()
	ev := nostr.Event{Kind: EnvelopeKind, CreatedAt: nostr.Now(), Tags: nostr.Tags{{"p", mailbox}}, Content: content}
	if err := ev.Sign(nostr.GeneratePrivateKey()); err != nil {
		t.Fatal(err)
	}
	return ev
}

func recv(c <-chan nostr.Event, d time.Duration) (nostr.Event, bool) {
	select {
	case ev := <-c:
		return ev, true
	case <-time.After(d):
		return nostr.Event{}, false
	}
}

func TestMemBusDelivers(t *testing.T) {
	bus := NewMemBus()
	a, b := bus.Join(), bus.Join()
	b.Subscribe([]string{"mine"})
	ev := testEvent(t, "mine", "x")
	a.Publish(context.Background(), testEvent(t, "other", "y"))
	a.Publish(context.Background(), ev)
	got, ok := recv(b.Events(), time.Second)
	if !ok || got.ID != ev.ID {
		t.Fatalf("want %s got %v %v", ev.ID, got.ID, ok)
	}
	if _, ok := recv(b.Events(), 50*time.Millisecond); ok {
		t.Fatal("unsubscribed tag delivered")
	}
	b.Close()
	if _, ok := <-b.Events(); ok {
		t.Fatal("channel not closed")
	}
}

func TestMemBusDedup(t *testing.T) {
	bus := NewMemBus()
	a, b := bus.Join(), bus.Join()
	b.Subscribe([]string{"mine"})
	ev := testEvent(t, "mine", "x")
	a.Publish(context.Background(), ev)
	a.Publish(context.Background(), ev)
	recv(b.Events(), time.Second)
	if _, ok := recv(b.Events(), 50*time.Millisecond); ok {
		t.Fatal("duplicate delivered")
	}
}

func TestLANLoopback(t *testing.T) {
	a, err := NewLAN()
	if err != nil {
		t.Skip("multicast unavailable:", err)
	}
	defer a.Close()
	b, err := NewLAN()
	if err != nil {
		t.Skip("multicast unavailable:", err)
	}
	defer b.Close()
	b.Subscribe([]string{"mine"})
	ev := testEvent(t, "mine", "hello")
	if err := a.Publish(context.Background(), ev); err != nil {
		t.Skip("multicast send failed:", err)
	}
	got, ok := recv(b.Events(), 2*time.Second)
	if !ok {
		t.Skip("no multicast loopback in this environment")
	}
	if got.ID != ev.ID {
		t.Fatal("wrong event")
	}
}
