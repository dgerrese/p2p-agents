//go:build live

package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func liveNode(t *testing.T, lanOnly bool) (*Node, string) {
	t.Helper()
	st := &Store{Dir: t.TempDir()}
	cfg, _ := st.Config()
	cfg.LANOnly = lanOnly
	if err := st.SaveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	n, err := NewNodeFromConfig(st)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return n, n.pub
}

func liveWait(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(500 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timeout (%v) waiting for %s", d, what)
}

func livePair(t *testing.T, a, b *Node) {
	t.Helper()
	code, err := a.Invite(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bid, err := b.Join(code)
	if err != nil {
		t.Fatal(err)
	}
	var aid string
	liveWait(t, 90*time.Second, "pair request on both sides", func() bool {
		ra := a.Requests()
		if len(ra) > 0 {
			aid = ra[0].ID
		}
		return len(ra) > 0 && len(b.Requests()) > 0
	})
	ca, _ := a.VerifyCodeFor(aid)
	cb, _ := b.VerifyCodeFor(bid)
	if ca == "" || ca != cb {
		t.Fatalf("verify codes differ: %q vs %q", ca, cb)
	}
	if err := a.Accept(aid, ca); err != nil {
		t.Fatal(err)
	}
	if err := b.Accept(bid, cb); err != nil {
		t.Fatal(err)
	}
	liveWait(t, 90*time.Second, "paired", func() bool {
		pa, _ := a.st.Peers()
		pb, _ := b.st.Peers()
		return len(pa) == 1 && len(pb) == 1
	})
}

func liveSend(t *testing.T, from, to *Node, text string) {
	t.Helper()
	liveWait(t, 90*time.Second, "peer online", func() bool {
		ps := from.Peers()
		return len(ps) == 1 && ps[0].Online
	})
	peer := from.Peers()[0].Pub
	id, status, err := from.Send(context.Background(), peer, text, "")
	if err != nil || status != "delivered" {
		t.Fatalf("send: id=%s status=%q err=%v", id, status, err)
	}
	t.Logf("%s -> delivered", text)
}

func liveRoundTrip(t *testing.T, lanOnly bool) {
	t.Setenv("P2PA_DEBUG", "1")
	if !lanOnly {
		t.Setenv("P2PA_NO_LAN", "1")
	}
	var mu sync.Mutex
	var seen []nostr.Event
	old := debugHook
	debugHook = func(dir string, ev nostr.Event) {
		mu.Lock()
		seen = append(seen, ev)
		mu.Unlock()
		old(dir, ev)
	}
	t.Cleanup(func() { debugHook = old }) // registered before the nodes, so it runs after they shut down

	a, aPub := liveNode(t, lanOnly)
	b, bPub := liveNode(t, lanOnly)
	livePair(t, a, b)
	liveSend(t, a, b, "ping from a")
	liveSend(t, b, a, "pong from b")

	mu.Lock()
	defer mu.Unlock()
	if len(seen) == 0 {
		t.Fatal("no outer events captured")
	}
	size := len(seen[0].Content)
	for _, ev := range seen {
		raw, _ := json.Marshal(ev)
		if strings.Contains(string(raw), aPub) || strings.Contains(string(raw), bPub) {
			t.Fatalf("identity pub visible in outer event %s", ev.ID)
		}
		if ev.Kind != 21059 {
			t.Fatalf("kind %d", ev.Kind)
		}
		if len(ev.Content) != size {
			t.Fatalf("content length %d != %d", len(ev.Content), size)
		}
	}
	t.Logf("%d outer events: no identity pub, kind 21059, content length %d each", len(seen), size)
}

func TestLiveRelays(t *testing.T)  { liveRoundTrip(t, false) }
func TestLiveLANOnly(t *testing.T) { liveRoundTrip(t, true) }

// Task 3 assumption: re-subscribing or closing ends the old SubscribeMany goroutine.
// The loop goroutine (pool.subMany per-relay worker) must exit; goroutines parked in
// go-nostr's EOSE waiter (subMany.func2.3) are counted separately.
func TestLiveRelayResubscribeNoLeak(t *testing.T) {
	cfg, _ := (&Store{Dir: t.TempDir()}).Config()
	stacks := func() (total, workers, eose int) {
		buf := make([]byte, 1<<20)
		dump := string(buf[:runtime.Stack(buf, true)])
		return runtime.NumGoroutine(), strings.Count(dump, "(*relaySignal).Subscribe.func1"), strings.Count(dump, "subMany.func2.3")
	}
	for _, n := range []int{5, 20} {
		before, _, _ := stacks()
		r := NewRelays(cfg.Relays)
		for i := 0; i < n; i++ {
			r.Subscribe([]string{fmt.Sprintf("%064x", i+1)})
			time.Sleep(300 * time.Millisecond)
		}
		mid, midW, midE := stacks()
		r.Close()
		time.Sleep(10 * time.Second)
		after, afterW, afterE := stacks()
		t.Logf("%2d subscribes: goroutines before=%d mid=%d after Close=%d | relaySignal reader loops mid=%d after=%d | eose waiters mid=%d after=%d",
			n, before, mid, after, midW, afterW, midE, afterE)
		if afterW != 0 {
			t.Errorf("%d Subscribe reader goroutines survive Close", afterW)
		}
	}
}
