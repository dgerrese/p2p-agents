package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type testNode struct {
	*Node
	st *Store
	mu sync.Mutex
	ev []Event
}

func newTestNode(t *testing.T, bus *MemBus) *testNode {
	t.Helper()
	tn := newIdleNode(t, bus)
	tn.start(t)
	return tn
}

// newIdleNode builds a node whose Run loop is not started; envelopes queue in its signal.
func newIdleNode(t *testing.T, bus *MemBus) *testNode {
	t.Helper()
	t.Setenv("P2PA_HOME", t.TempDir())
	st, err := OpenStore()
	if err != nil {
		t.Fatal(err)
	}
	n, err := NewNode(st, bus.Join(), nil)
	if err != nil {
		t.Fatal(err)
	}
	n.stun = nil // no public STUN once paired nodes start dialing
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

func (tn *testNode) start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { tn.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

func (tn *testNode) events() []Event {
	tn.mu.Lock()
	defer tn.mu.Unlock()
	return append([]Event(nil), tn.ev...)
}

func (tn *testNode) waitEvent(t *testing.T, kind string) Event {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		for _, e := range tn.events() {
			if e.Kind == kind {
				return e
			}
		}
	}
	t.Fatalf("no %q event; got %+v", kind, tn.events())
	return Event{}
}

func (tn *testNode) peers(t *testing.T) []Peer {
	t.Helper()
	ps, err := tn.st.Peers()
	if err != nil {
		t.Fatal(err)
	}
	return ps
}

// pairUp runs invite+join and returns the request IDs on each side.
func pairUp(t *testing.T, a, b *testNode) (aID, bID string) {
	t.Helper()
	code, err := a.Invite(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if bID, err = b.Join(code); err != nil {
		t.Fatal(err)
	}
	aID = a.waitEvent(t, "pair_request").ID
	if got := b.waitEvent(t, "pair_request").ID; got != bID {
		t.Fatalf("joiner request id %q, Join returned %q", got, bID)
	}
	return aID, bID
}

// accept plays n's owner: it types the code the peer's owner reads out (peer's shown code).
func accept(t *testing.T, n *testNode, id string, peer *testNode, peerID string) {
	t.Helper()
	c, err := peer.VerifyCodeFor(peerID)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Accept(id, c); err != nil {
		t.Fatal(err)
	}
}

func TestPairHappyPath(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, bID := pairUp(t, a, b)
	ca, _ := a.VerifyCodeFor(aID)
	cb, _ := b.VerifyCodeFor(bID)
	if ca == cb || len(ca) != 6 || len(cb) != 6 {
		t.Fatalf("codes %q %q", ca, cb)
	}
	if rs := a.Requests(); len(rs) != 1 || rs[0].ID != aID || rs[0].PeerPub == "" {
		t.Fatalf("A requests %+v", rs)
	}
	accept(t, a, aID, b, bID)
	accept(t, b, bID, a, aID)
	a.waitEvent(t, "paired")
	b.waitEvent(t, "paired")
	pa, pb := a.peers(t), b.peers(t)
	_, aPub, _ := a.st.Identity()
	_, bPub, _ := b.st.Identity()
	if len(pa) != 1 || len(pb) != 1 || pa[0].Pub != bPub || pb[0].Pub != aPub {
		t.Fatalf("peers A=%+v B=%+v", pa, pb)
	}
	if string(pa[0].Secret) != string(pb[0].Secret) || len(pa[0].Secret) == 0 {
		t.Fatal("pair secrets differ")
	}
	if len(a.Requests()) != 0 || len(b.Requests()) != 0 {
		t.Fatal("requests not cleared")
	}
	a.resubscribe()
	a.mu.Lock()
	defer a.mu.Unlock()
	w := Window(now())
	for _, win := range []int64{w - 1, w, w + 1} {
		if _, pub := MailboxKey(pa[0].Secret, win); a.boxes[pub].peer != bPub {
			t.Fatalf("A not subscribed to peer mailbox for window %d", win)
		}
	}
	if len(a.boxes) != 3 {
		t.Fatalf("A still subscribed to pairing mailboxes: %d boxes", len(a.boxes))
	}
}

func TestPairWrongCode(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, bID := pairUp(t, a, b)
	c, _ := b.VerifyCodeFor(bID)
	own, _ := a.VerifyCodeFor(aID)
	wrong := "000000"
	for i := 1; wrong == c || wrong == own; i++ {
		wrong = fmt.Sprintf("%06d", i)
	}
	for i := 0; i < 3; i++ {
		if err := a.Accept(aID, wrong); !errors.Is(err, ErrWrongCode) {
			t.Fatalf("try %d: err %v", i, err)
		}
		if i < 2 && len(a.Requests()) != 1 {
			t.Fatal("request dropped too early")
		}
	}
	if len(a.Requests()) != 0 {
		t.Fatal("request still pending after 3 wrong codes")
	}
	a.waitEvent(t, "pair_failed")
	b.waitEvent(t, "pair_failed")
	if len(a.peers(t)) != 0 || len(b.peers(t)) != 0 {
		t.Fatal("paired despite wrong code")
	}
	if err := a.Accept(aID, c); err == nil {
		t.Fatal("accept after rejection succeeded")
	}
}

// TestPairOwnCodeRejected: typing the code on your own screen proves nothing, so it fails on both sides.
func TestPairOwnCodeRejected(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, bID := pairUp(t, a, b)
	for _, s := range []struct {
		n  *testNode
		id string
	}{{a, aID}, {b, bID}} {
		own, _ := s.n.VerifyCodeFor(s.id)
		if err := s.n.Accept(s.id, own); !errors.Is(err, ErrWrongCode) {
			t.Fatalf("own code accepted: %v", err)
		}
	}
	// the right codes still pair after one wasted try each
	accept(t, a, aID, b, bID)
	accept(t, b, bID, a, aID)
	a.waitEvent(t, "paired")
	b.waitEvent(t, "paired")
}

func TestPairCodesDiffer(t *testing.T) {
	bus := NewMemBus()
	a := newTestNode(t, bus)
	for i := 0; i < 5; i++ {
		b := newTestNode(t, bus)
		code, err := a.Invite(time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		bID, err := b.Join(code)
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, 2*time.Second, "codes on both sides", func() bool { return len(a.Requests()) == i+1 && len(b.Requests()) == 1 })
		var aID string
		for _, r := range a.Requests() {
			if _, pub, _ := b.st.Identity(); r.PeerPub == pub {
				aID = r.ID
			}
		}
		ca, _ := a.VerifyCodeFor(aID)
		cb, _ := b.VerifyCodeFor(bID)
		if ca == cb || len(ca) != 6 || len(cb) != 6 {
			t.Fatalf("pairing %d: codes %q %q", i, ca, cb)
		}
	}
}

func TestPairOneSidedAccept(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, bID := pairUp(t, a, b)
	accept(t, a, aID, b, bID)
	time.Sleep(100 * time.Millisecond)
	if len(a.peers(t)) != 0 || len(b.peers(t)) != 0 {
		t.Fatal("paired with only one side accepting")
	}
}

func TestPairReject(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, _ := pairUp(t, a, b)
	if err := a.Reject(aID); err != nil {
		t.Fatal(err)
	}
	b.waitEvent(t, "pair_failed")
	if len(a.Requests()) != 0 || len(b.Requests()) != 0 {
		t.Fatal("requests remain after reject")
	}
}

func TestInviteReuse(t *testing.T) {
	bus := NewMemBus()
	a, b, c := newTestNode(t, bus), newTestNode(t, bus), newTestNode(t, bus)
	code, err := a.Invite(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Join(code); err != nil {
		t.Fatal(err)
	}
	b.waitEvent(t, "pair_request")
	if _, err := c.Join(code); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(c.events()) != 0 || len(c.Requests()) != 0 {
		t.Fatalf("C got a response: %+v", c.events())
	}
	if n := len(a.Requests()); n != 1 {
		t.Fatalf("A has %d requests", n)
	}
}

func TestInviteExpired(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	code, err := a.Invite(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	// Forge a far-future expiry so B sends anyway; A must enforce its own.
	raw, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(code, "p2pa1:"))
	var inv map[string]any
	json.Unmarshal(raw, &inv)
	inv["e"] = time.Now().Add(time.Hour).Unix()
	raw, _ = json.Marshal(inv)
	if _, err := b.Join("p2pa1:" + base64.RawURLEncoding.EncodeToString(raw)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if len(a.Requests()) != 0 || len(a.events()) != 0 {
		t.Fatal("A accepted an expired invite")
	}
	if _, err := b.Join(code); err == nil {
		t.Fatal("Join accepted an expired code")
	}
}

func TestEventsNeverContainCode(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, bID := pairUp(t, a, b)
	ca, _ := a.VerifyCodeFor(aID)
	cb, _ := b.VerifyCodeFor(bID)
	accept(t, a, aID, b, bID)
	accept(t, b, bID, a, aID)
	a.waitEvent(t, "paired")
	b.waitEvent(t, "paired")
	for _, e := range append(a.events(), b.events()...) {
		j, _ := json.Marshal(e)
		if strings.Contains(string(j), ca) || strings.Contains(string(j), cb) {
			t.Fatalf("event leaks code: %s", j)
		}
	}
}

// TestConcurrentJoiners: B and C both join before A handles either; C must never bind to A.
func TestConcurrentJoiners(t *testing.T) {
	bus := NewMemBus()
	a := newIdleNode(t, bus)
	b, c := newTestNode(t, bus), newTestNode(t, bus)
	code, err := a.Invite(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bID, err := b.Join(code)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Join(code); err != nil {
		t.Fatal(err)
	}
	a.start(t)
	aID := a.waitEvent(t, "pair_request").ID
	b.waitEvent(t, "pair_request")
	accept(t, a, aID, b, bID)
	accept(t, b, bID, a, aID)
	a.waitEvent(t, "paired")
	b.waitEvent(t, "paired")
	time.Sleep(100 * time.Millisecond)
	if len(c.events()) != 0 || len(c.Requests()) != 0 || len(c.peers(t)) != 0 {
		t.Fatalf("C bound to A: events %+v", c.events())
	}
}

// handJoin plays a joiner by hand against a and sends reveal(nB) as pair_reveal.
func handJoin(t *testing.T, bus *MemBus, a *testNode, reveal func(nB []byte) []byte) {
	t.Helper()
	code, err := a.Invite(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(code, invitePrefix))
	var inv inviteCode
	json.Unmarshal(raw, &inv)
	jsk := nostr.GeneratePrivateKey()
	jpub, _ := nostr.GetPublicKey(jsk)
	sig := bus.Join()
	defer sig.Close()
	rsk, rpub := MailboxKey(inv.S, replyWindow)
	sig.Subscribe([]string{rpub})
	send := func(typ string, m pairMsg) {
		d, _ := json.Marshal(m)
		ev, err := Seal(Payload{Type: typ, Data: d}, jsk, inv.M)
		if err != nil {
			t.Fatal(err)
		}
		sig.Publish(context.Background(), ev)
	}
	nB := randBytes(32)
	commit := sha256.Sum256(nB)
	send("pair_req", pairMsg{Name: "mallory", Commit: commit[:], MAC: pairMAC(inv.S, "req", jpub)})
	ev, ok := recv(sig.Events(), 2*time.Second)
	if !ok {
		t.Fatal("no pair_resp")
	}
	p, apub, _, err := Open(ev, rsk)
	if err != nil || p.Type != "pair_resp" {
		t.Fatalf("got %v %v", p.Type, err)
	}
	if len(a.Requests()) != 0 {
		t.Fatal("inviter shows request before reveal")
	}
	send("pair_reveal", pairMsg{Nonce: reveal(nB), MAC: pairMAC(inv.S, "reveal", jpub, apub)})
	time.Sleep(100 * time.Millisecond)
}

func TestPairWrongReveal(t *testing.T) {
	bus := NewMemBus()
	a := newTestNode(t, bus)
	handJoin(t, bus, a, func([]byte) []byte { return randBytes(32) })
	if len(a.Requests()) != 0 || len(a.events()) != 0 || len(a.peers(t)) != 0 {
		t.Fatalf("wrong reveal produced a request: %+v", a.events())
	}
	// Control: the same hand-played flow with the right nonce does surface a request.
	handJoin(t, bus, a, func(nB []byte) []byte { return nB })
	waitFor(t, 2*time.Second, "request from correct reveal", func() bool { return len(a.Requests()) == 1 })
	if e := a.waitEvent(t, "pair_request"); e.Name != "mallory" {
		t.Fatalf("event %+v", e)
	}
}

func TestPairRequestExpires(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	aID, _ := pairUp(t, a, b)
	a.mu.Lock()
	a.reqs[aID].Expires = now().Add(-time.Second)
	a.mu.Unlock()
	a.resubscribe()
	if e := a.waitEvent(t, "pair_failed"); e.ID != aID || e.Reason != "expired" {
		t.Fatalf("event %+v", e)
	}
	if len(a.Requests()) != 0 {
		t.Fatal("expired request still listed")
	}
}

func TestCleanName(t *testing.T) {
	if got := cleanName("Bob\x1b[31m\u202e\n" + strings.Repeat("x", 100)); got != "Bob[31m"+strings.Repeat("x", 57) {
		t.Fatalf("got %q", got)
	}
}

// TestJoinOversizedNameError: a seal failure surfaces as such, not as a malformed code.
func TestJoinOversizedNameError(t *testing.T) {
	bus := NewMemBus()
	a, b := newTestNode(t, bus), newTestNode(t, bus)
	code, err := a.Invite(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	b.name = strings.Repeat("x", PadSize)
	if _, err := b.Join(code); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("err = %v", err)
	}
}
