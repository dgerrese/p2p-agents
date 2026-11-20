package p2p

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/nbd-wtf/go-nostr"
)

// debugHook sees every outer event published ("out") or received ("in") when P2PA_DEBUG=1.
// The default logs one JSON line to stderr; tests replace it.
var debugHook = func(dir string, ev nostr.Event) {
	b, _ := json.Marshal(map[string]any{"dir": dir, "id": ev.ID, "kind": ev.Kind, "pubkey": ev.PubKey,
		"tags": ev.Tags, "content_len": len(ev.Content)})
	log.Printf("p2pa-debug %s", b)
}

type debugSignal struct {
	Signal
	out chan nostr.Event
}

func debugWrap(s Signal) Signal {
	d := &debugSignal{Signal: s, out: make(chan nostr.Event, 256)}
	go func() {
		defer close(d.out)
		for ev := range s.Events() {
			debugHook("in", ev)
			select {
			case d.out <- ev:
			default: // drop like the inbox when the consumer lags
			}
		}
	}()
	return d
}

func (d *debugSignal) Publish(ctx context.Context, ev nostr.Event) error {
	debugHook("out", ev)
	return d.Signal.Publish(ctx, ev)
}

func (d *debugSignal) Events() <-chan nostr.Event { return d.out }

func debugOn() bool { return os.Getenv("P2PA_DEBUG") == "1" }
