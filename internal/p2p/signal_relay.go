package p2p

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type relaySignal struct {
	*inbox
	urls      []string
	pool      *nostr.SimplePool
	ctx       context.Context
	cancel    context.CancelFunc
	subMu     sync.Mutex
	cancelSub context.CancelFunc
}

func NewRelays(urls []string) Signal {
	ctx, cancel := context.WithCancel(context.Background())
	return &relaySignal{inbox: newInbox(), urls: urls, pool: nostr.NewSimplePool(ctx), ctx: ctx, cancel: cancel}
}

// Publish succeeds if at least one relay accepted the event.
func (r *relaySignal) Publish(ctx context.Context, ev nostr.Event) error {
	var errs []error
	for res := range r.pool.PublishMany(ctx, r.urls, ev) {
		if res.Error == nil {
			return nil // ponytail: remaining relays finish in the background, results unread
		}
		errs = append(errs, res.Error)
	}
	return errors.Join(append(errs, errors.New("no relay accepted event"))...)
}

func (r *relaySignal) Subscribe(pubs []string) {
	r.setSubs(pubs)
	r.subMu.Lock()
	defer r.subMu.Unlock()
	if r.cancelSub != nil {
		r.cancelSub()
		r.cancelSub = nil
	}
	if len(pubs) == 0 || r.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.ctx)
	r.cancelSub = cancel
	since := nostr.Timestamp(time.Now().Add(-60 * time.Second).Unix())
	ch := r.pool.SubscribeMany(ctx, r.urls, nostr.Filter{
		Kinds: []int{EnvelopeKind},
		Tags:  nostr.TagMap{"p": pubs},
		Since: &since,
	})
	go func() {
		for re := range ch {
			ev := *re.Event
			if ev.Kind != EnvelopeKind {
				continue
			}
			if ok, err := ev.CheckSignature(); err != nil || !ok {
				continue
			}
			r.deliver(ev)
		}
	}()
}

func (r *relaySignal) Events() <-chan nostr.Event { return r.out }

func (r *relaySignal) Close() error {
	r.cancel()
	r.close()
	return nil
}
