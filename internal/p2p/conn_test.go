package p2p

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func init() { UseLoopbackICE() }

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
	ps, _ := tn.Peers()
	for _, p := range ps {
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
	ps := peersOf(t, a)
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
	if conns != 0 || len(b.events()) != 0 || len(peersOf(t, b)) != 1 || peersOf(t, b)[0].Pub != aPub {
		t.Fatalf("conns=%d events=%+v peers=%+v", conns, b.events(), peersOf(t, b))
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
	if len(peersOf(t, a)) != 0 || len(peersOf(t, b)) != 0 {
		t.Fatalf("peers left: A=%+v B=%+v", peersOf(t, a), peersOf(t, b))
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
	if len(peersOf(t, b)) != 0 {
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

// TestExtraDataChannelsClosed: the answerer keeps only the offerer's first "p2pa" channel.
func TestExtraDataChannelsClosed(t *testing.T) {
	a, _, _, bPub := connectedPair(t) // A offers, B answers
	a.mu.Lock()
	pc := a.conns[bPub].pc
	a.mu.Unlock()
	for _, label := range []string{"evil", "p2pa"} {
		dc, err := pc.CreateDataChannel(label, nil)
		if err != nil {
			t.Fatal(err)
		}
		closed := make(chan struct{})
		dc.OnClose(func() { close(closed) })
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("extra %q channel left open by the answerer", label)
		}
	}
	if !a.online(bPub) {
		t.Fatal("main channel dropped")
	}
}

type countSig struct {
	Signal
	n atomic.Int32
}

func (c *countSig) Publish(ctx context.Context, ev nostr.Event) error {
	c.n.Add(1)
	return c.Signal.Publish(ctx, ev)
}

// TestLANFirstConnect: on a shared LAN, offer and answer never reach the relays; only B's knock does.
func TestLANFirstConnect(t *testing.T) {
	sa, sb, aPub, bPub := pairedStores(t)
	lan, relays := NewMemBus(), NewMemBus()
	var nodes [2]*testNode
	var rel [2]*countSig
	for i, st := range []*Store{sa, sb} {
		rel[i] = &countSig{Signal: relays.Join()}
		n, err := NewNode(st, lan.Join(), rel[i])
		if err != nil {
			t.Fatal(err)
		}
		n.stun = nil
		nodes[i] = &testNode{Node: n, st: st}
		go func() {
			for range n.Events() {
			}
		}()
		run(t, nodes[i])
	}
	waitFor(t, 10*time.Second, "connection", func() bool { return nodes[0].online(bPub) && nodes[1].online(aPub) })
	time.Sleep(lanWait + time.Second) // a fallback would have gone out by now
	if a, b := rel[0].n.Load(), rel[1].n.Load(); a != 0 || b != 1 {
		t.Fatalf("relay publishes: A=%d (want 0) B=%d (want 1, the knock)", a, b)
	}
}

// TestPairThenMessage chains pairing and connecting: pair over the protocol, then message.
func TestPairThenMessage(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, bID := pairUp(t, a, b)
	accept(t, a, aID)
	accept(t, b, bID)
	a.waitEvent(t, "paired")
	b.waitEvent(t, "paired")
	_, aPub, _ := a.st.Identity()
	_, bPub, _ := b.st.Identity()
	waitFor(t, 15*time.Second, "connection", func() bool { return a.online(bPub) && b.online(aPub) })
	id, status, err := b.Send(context.Background(), aPub, "hi from b", "")
	if err != nil || status != "delivered" {
		t.Fatalf("send: %q %v", status, err)
	}
	if e := a.waitEvent(t, "message"); e.Text != "hi from b" || e.Peer != bPub || e.ID != id {
		t.Fatalf("event %+v", e)
	}
}

func peersOf(t *testing.T, n interface{ Peers() ([]PeerStatus, error) }) []PeerStatus {
	t.Helper()
	ps, err := n.Peers()
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

func TestPeersSurfacesStoreError(t *testing.T) {
	tn := newIdleNode(t, NewMemBus())
	if err := os.WriteFile(filepath.Join(tn.st.Dir, "peers.json"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if ps, err := tn.Peers(); err == nil {
		t.Fatalf("corrupt peers.json: got %+v, nil error", ps)
	}
}

func rawMsg(t *testing.T, a *testNode, bPub string, m wireMsg) string {
	t.Helper()
	ack := make(chan string, 1)
	a.Node.mu.Lock()
	a.acks[m.ID] = ack
	dc := a.conns[bPub].dc
	a.Node.mu.Unlock()
	if err := sendWire(dc, m); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-ack:
		return s
	case <-time.After(5 * time.Second):
		t.Fatal("no ack")
	}
	return ""
}

func TestAckTimeoutUnconfirmed(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	a.ackTimeout = 300 * time.Millisecond
	b.Node.mu.Lock()
	delete(b.known, aPub) // b now ignores a's frames, so no ack comes back
	b.Node.mu.Unlock()
	id, status, err := a.Send(context.Background(), bPub, "maybe", "")
	if err != nil || status != "unconfirmed" {
		t.Fatalf("status=%q err=%v", status, err)
	}
	l, _ := a.st.ReadLog(bPub, time.Time{})
	if len(l) != 1 || l[0].ID != id || l[0].Status != "unconfirmed" {
		t.Fatalf("log %+v", l)
	}
}

func TestDuplicateIncomingIgnored(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	m := wireMsg{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: "once"}
	for i := 0; i < 2; i++ {
		if s := rawMsg(t, a, bPub, m); s != "delivered" {
			t.Fatalf("send %d: ack %q", i, s)
		}
	}
	if n := b.count("message"); n != 1 {
		t.Fatalf("%d message events", n)
	}
	if l, _ := b.st.ReadLog(aPub, time.Time{}); len(l) != 1 {
		t.Fatalf("%d log entries", len(l))
	}
}

func TestIncomingRateLimit(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	for i := 0; i < rateBurst+3; i++ {
		s := rawMsg(t, a, bPub, wireMsg{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: "x"})
		if want := map[bool]string{true: "delivered", false: "failed"}[i < rateBurst]; s != want {
			t.Fatalf("msg %d: ack %q, want %q", i, s, want)
		}
	}
	if l, _ := b.st.ReadLog(aPub, time.Time{}); len(l) != rateBurst {
		t.Fatalf("%d log entries", len(l))
	}
	if n := b.count("message"); n != rateBurst {
		t.Fatalf("%d events", n)
	}
}

func TestBucketRefill(t *testing.T) {
	var b bucket
	t0 := time.Unix(1000, 0)
	for i := 0; i < rateBurst; i++ {
		if !b.allow(t0) {
			t.Fatalf("burst %d denied", i)
		}
	}
	if b.allow(t0) || b.allow(t0.Add(time.Second)) { // 1s refills only half a token
		t.Fatal("over-limit allowed")
	}
	if !b.allow(t0.Add(2 * time.Second)) { // a full token by now
		t.Fatal("not refilled")
	}
	if b.allow(t0.Add(2 * time.Second)) {
		t.Fatal("extra token")
	}
	b.allow(t0.Add(time.Hour))
	if b.tokens > rateBurst {
		t.Fatalf("tokens %v above burst", b.tokens)
	}
}

func TestDuplicatesChargedAndLimitedNotSeen(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	first := wireMsg{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: "x"}
	for i := 0; i < rateBurst; i++ {
		m := first
		if i > 0 {
			m.ID = hex.EncodeToString(randBytes(16))
		}
		if s := rawMsg(t, a, bPub, m); s != "delivered" {
			t.Fatalf("msg %d: %q", i, s)
		}
	}
	if s := rawMsg(t, a, bPub, first); s != "failed" { // duplicate over the limit
		t.Fatalf("duplicate over limit: %q", s)
	}
	over := wireMsg{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: "y"}
	if s := rawMsg(t, a, bPub, over); s != "failed" {
		t.Fatalf("over: %q", s)
	}
	b.Node.mu.Lock()
	defer b.Node.mu.Unlock()
	if b.recv[aPub][over.ID] || !b.recv[aPub][first.ID] {
		t.Fatal("seen set wrong")
	}
}

func TestUnpairClearsPeerLimits(t *testing.T) {
	a, b, aPub, bPub := connectedPair(t)
	if s := rawMsg(t, a, bPub, wireMsg{ID: hex.EncodeToString(randBytes(16)), Type: "msg", Text: "x"}); s != "delivered" {
		t.Fatal(s)
	}
	b.Node.mu.Lock()
	has := b.recv[aPub] != nil && b.rate[aPub] != nil
	b.Node.mu.Unlock()
	if !has {
		t.Fatal("state not recorded")
	}
	if err := b.removePeer(aPub, "test"); err != nil {
		t.Fatal(err)
	}
	b.Node.mu.Lock()
	defer b.Node.mu.Unlock()
	if _, ok := b.recv[aPub]; ok {
		t.Fatal("recv kept")
	}
	if _, ok := b.rate[aPub]; ok {
		t.Fatal("rate kept")
	}
}

func TestBucketBackwardClock(t *testing.T) {
	var b bucket
	t0 := time.Unix(1000, 0)
	for i := 0; i < rateBurst; i++ {
		b.allow(t0)
	}
	b.allow(t0.Add(-time.Hour))
	if b.tokens < 0 || b.tokens >= 1 {
		t.Fatalf("tokens %v", b.tokens)
	}
}
