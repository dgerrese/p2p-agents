package p2p

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// Loopback-only IPv4 host candidates: fast and deterministic ICE on machines with many interfaces.
func init() {
	rtcAPI = newRTCAPI(func(se *webrtc.SettingEngine) {
		se.SetIncludeLoopbackCandidate(true)
		se.SetInterfaceFilter(func(name string) bool { return strings.HasPrefix(name, "lo") })
		se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	})
}

// pairedStores returns two stores that already know each other, with A's pub < B's pub (A offers).
func pairedStores(t *testing.T) (a, b *Store, aPub, bPub string) {
	t.Helper()
	a, b = &Store{Dir: t.TempDir()}, &Store{Dir: t.TempDir()}
	_, aPub, _ = a.Identity()
	_, bPub, _ = b.Identity()
	if aPub > bPub {
		a, b, aPub, bPub = b, a, bPub, aPub
	}
	secret := randBytes(32)
	if err := a.AddPeer(Peer{Pub: bPub, Name: "bob", Secret: secret}); err != nil {
		t.Fatal(err)
	}
	if err := b.AddPeer(Peer{Pub: aPub, Name: "alice", Secret: secret}); err != nil {
		t.Fatal(err)
	}
	return
}

// nodeOn builds a node for st on bus, with no STUN (host candidates only), without running it.
func nodeOn(t *testing.T, st *Store, bus *MemBus) *testNode {
	t.Helper()
	n, err := NewNode(st, bus.Join(), nil)
	if err != nil {
		t.Fatal(err)
	}
	n.stun = nil
	tn := &testNode{Node: n, st: st}
	go func() {
		for e := range n.Events() {
			tn.mu.Lock()
			tn.ev = append(tn.ev, e)
			tn.mu.Unlock()
		}
	}()
	return tn
}

// run starts tn and returns a stop func that waits for Run to return (also run at cleanup).
func run(t *testing.T, tn *testNode) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { tn.Run(ctx); close(done) }()
	stop = func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func (tn *testNode) count(kind string) int {
	c := 0
	for _, e := range tn.events() {
		if e.Kind == kind {
			c++
		}
	}
	return c
}

func (tn *testNode) online(pub string) bool {
	for _, p := range tn.Peers() {
		if p.Pub == pub {
			return p.Online
		}
	}
	return false
}

func connectedPair(t *testing.T) (a, b *testNode, aPub, bPub string) {
	t.Helper()
	sa, sb, aPub, bPub := pairedStores(t)
	bus := NewMemBus()
	a, b = nodeOn(t, sa, bus), nodeOn(t, sb, bus)
	run(t, a)
	run(t, b)
	waitFor(t, 10*time.Second, "connection", func() bool { return a.online(bPub) && b.online(aPub) })
	return
}

func TestMessageRoundTrip(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	id, status, err := a.Send(context.Background(), bPub, "hello bob", "")
	if err != nil || status != "delivered" || len(id) != 32 {
		t.Fatalf("send: id=%q status=%q err=%v", id, status, err)
	}
	e := b.waitEvent(t, "message")
	if e.Text != "hello bob" || e.Peer != aPub || e.Name != "alice" || e.ID != id {
		t.Fatalf("event %+v", e)
	}
	la, _ := a.st.ReadLog(bPub, time.Time{})
	lb, _ := b.st.ReadLog(aPub, time.Time{})
	if len(la) != 1 || la[0].Dir != "out" || la[0].Text != "hello bob" || la[0].ID != id {
		t.Fatalf("A log %+v", la)
	}
	if len(lb) != 1 || lb[0].Dir != "in" || lb[0].Text != "hello bob" || lb[0].Peer != aPub {
		t.Fatalf("B log %+v", lb)
	}
	ps := a.Peers()
	if len(ps) != 1 || ps[0].Name != "bob" || !ps[0].Online || ps[0].Fingerprint != bPub[0:4]+" "+bPub[4:8]+" "+bPub[8:12]+" "+bPub[12:16] {
		t.Fatalf("peers %+v", ps)
	}
}

func TestGlareSingleConnection(t *testing.T) {
	for i := 0; i < 20; i++ {
		sa, sb, aPub, bPub := pairedStores(t)
		bus := NewMemBus()
		a, b := nodeOn(t, sa, bus), nodeOn(t, sb, bus)
		first, second := a, b
		if i%2 == 1 {
			first, second = b, a
		}
		stop1, stop2 := run(t, first), run(t, second)
		waitFor(t, 10*time.Second, "connection", func() bool { return a.online(bPub) && b.online(aPub) })
		if _, st, err := b.Send(context.Background(), aPub, "hi", ""); st != "delivered" || err != nil {
			t.Fatalf("iter %d: send %q %v", i, st, err)
		}
		time.Sleep(200 * time.Millisecond) // let any second connection surface
		if ca, cb := a.connected.Load(), b.connected.Load(); ca != 1 || cb != 1 {
			t.Fatalf("iter %d: connected PCs A=%d B=%d", i, ca, cb)
		}
		stop1()
		stop2()
	}
}

func TestSendOffline(t *testing.T) {
	sa, _, _, bPub := pairedStores(t)
	a := nodeOn(t, sa, NewMemBus())
	run(t, a)
	start := time.Now()
	_, status, err := a.Send(context.Background(), bPub, "anyone?", "")
	if err != nil || status != "offline" || time.Since(start) > 11*time.Second {
		t.Fatalf("status=%q err=%v after %v", status, err, time.Since(start))
	}
}

func TestReconnect(t *testing.T) {
	sa, sb, aPub, bPub := pairedStores(t)
	bus := NewMemBus()
	a, b := nodeOn(t, sa, bus), nodeOn(t, sb, bus)
	run(t, a)
	stopB := run(t, b)
	waitFor(t, 10*time.Second, "connection", func() bool { return a.online(bPub) && b.online(aPub) })
	stopB()
	waitFor(t, 15*time.Second, "peer_offline", func() bool { return a.count("peer_offline") == 1 })
	b2 := nodeOn(t, sb, bus) // same store dir, as with the same P2PA_HOME
	run(t, b2)
	waitFor(t, 15*time.Second, "peer_online again", func() bool { return a.count("peer_online") == 2 })
	if _, st, err := a.Send(context.Background(), bPub, "back?", ""); st != "delivered" || err != nil {
		t.Fatalf("send after reconnect: %q %v", st, err)
	}
}

func TestUnknownSenderDropped(t *testing.T) {
	_, sb, aPub, _ := pairedStores(t)
	ps, _ := sb.Peers()
	bus := NewMemBus()
	b := nodeOn(t, sb, bus) // B is the non-offerer: it would answer an offer from A
	// C guesses a secret, and C also knows the real A-B secret but is not A.
	for _, secret := range [][]byte{randBytes(32), ps[0].Secret} {
		sc := &Store{Dir: t.TempDir()}
		if err := sc.AddPeer(Peer{Pub: ps[0].Pub, Name: "b", Secret: secret}); err != nil {
			t.Fatal(err)
		}
		c := nodeOn(t, sc, bus)
		_, mb := MailboxKey(secret, Window(now()))
		for _, typ := range []string{"offer", "knock", "bye"} {
			if err := c.publish(c.seal(typ, sdpMsg{SID: "x", SDP: "v=0"}, mb)); err != nil {
				t.Fatal(err)
			}
		}
	}
	run(t, b)
	time.Sleep(time.Second)
	b.mu.Lock()
	conns := len(b.conns)
	b.mu.Unlock()
	if conns != 0 || len(b.events()) != 0 || len(b.Peers()) != 1 || b.Peers()[0].Pub != aPub {
		t.Fatalf("conns=%d events=%+v peers=%+v", conns, b.events(), b.Peers())
	}
}

func TestUnpairNotifies(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	if err := a.Unpair(bPub, "testing"); err != nil {
		t.Fatal(err)
	}
	if e := a.waitEvent(t, "unpaired"); e.Peer != bPub {
		t.Fatalf("A event %+v", e)
	}
	if e := b.waitEvent(t, "unpaired"); e.Peer != aPub || e.Reason != "testing" {
		t.Fatalf("B event %+v", e)
	}
	if len(a.Peers()) != 0 || len(b.Peers()) != 0 {
		t.Fatalf("peers left: A=%+v B=%+v", a.Peers(), b.Peers())
	}
}

func TestUnpairOfflineNotifies(t *testing.T) {
	sa, sb, aPub, bPub := pairedStores(t)
	bus := NewMemBus()
	a, b := nodeOn(t, sa, bus), nodeOn(t, sb, bus) // B never runs: the bye envelope queues in its signal
	if err := a.Unpair(bPub, "bye"); err != nil {
		t.Fatal(err)
	}
	run(t, b)
	if e := b.waitEvent(t, "unpaired"); e.Peer != aPub {
		t.Fatalf("B event %+v", e)
	}
	if len(b.Peers()) != 0 {
		t.Fatal("B still has A")
	}
}

func TestOversizeRejected(t *testing.T) {
	_, b, aPub, _ := connectedPair(t)
	if _, _, err := b.Send(context.Background(), aPub, strings.Repeat("x", 65<<10), ""); err == nil {
		t.Fatal("65 KB send succeeded")
	}
}

// TestIncomingRejected writes bad frames straight onto the channel, bypassing Send's checks.
func TestIncomingRejected(t *testing.T) {
	a, b, _, bPub := connectedPair(t)
	for _, m := range []wireMsg{
		{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: strings.Repeat("x", 65<<10)},
		{ID: "not-hex", Type: "msg", Text: "hi"},
		{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: "hi", ReplyTo: "../x"},
	} {
		ack := make(chan string, 1)
		a.mu.Lock()
		a.acks[m.ID] = ack
		dc := a.conns[bPub].dc
		a.mu.Unlock()
		if err := sendWire(dc, m); err != nil {
			t.Fatal(err)
		}
		select {
		case s := <-ack:
			if s != "failed" {
				t.Fatalf("id %q: ack %q", m.ID, s)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("id %q: no ack", m.ID)
		}
	}
	if n := b.count("message"); n != 0 {
		t.Fatalf("B emitted %d message events", n)
	}
}

func TestLANOnlyNoRelays(t *testing.T) {
	t.Setenv("P2PA_NO_LAN", "1")
	dialed := 0
	old := newRelays
	t.Cleanup(func() { newRelays = old })
	newRelays = func([]string) Signal { dialed++; return NewMemBus().Join() }

	st := &Store{Dir: t.TempDir()}
	if err := st.SaveConfig(Config{LANOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewNodeFromConfig(st); err == nil || dialed != 0 {
		t.Fatalf("lan_only without LAN: err=%v relays built=%d", err, dialed)
	}
	if err := st.SaveConfig(Config{}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewNodeFromConfig(st); err != nil || dialed != 1 {
		t.Fatalf("default config: err=%v relays built=%d", err, dialed)
	}
}

func TestLANOnlyNoSTUN(t *testing.T) {
	for _, lanOnly := range []bool{true, false} {
		st := &Store{Dir: t.TempDir()}
		if err := st.SaveConfig(Config{LANOnly: lanOnly}); err != nil {
			t.Fatal(err)
		}
		n, err := NewNode(st, NewMemBus().Join(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := len(n.stun) == 0; got != lanOnly {
			t.Fatalf("lan_only=%v: stun=%v", lanOnly, n.stun)
		}
	}
}
