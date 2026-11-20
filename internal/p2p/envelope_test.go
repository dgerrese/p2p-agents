package p2p

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/nbd-wtf/go-nostr/nip44"
)

func newID(t *testing.T) (sk, pub string) {
	t.Helper()
	sk = nostr.GeneratePrivateKey()
	pub, err := nostr.GetPublicKey(sk)
	if err != nil {
		t.Fatal(err)
	}
	return sk, pub
}

func pl(n int) Payload {
	return Payload{Type: "offer", Data: json.RawMessage(`"` + strings.Repeat("x", n) + `"`)}
}

func TestSealOpenRoundTrip(t *testing.T) {
	isk, ipub := newID(t)
	msk, mpub := MailboxKey([]byte("s"), 1)
	ev, err := Seal(pl(10), isk, mpub)
	if err != nil {
		t.Fatal(err)
	}
	p, sender, _, err := Open(ev, msk)
	if err != nil || sender != ipub || p.Type != "offer" || string(p.Data) != string(pl(10).Data) {
		t.Fatalf("%v %v %v", p, sender, err)
	}
}

func TestSealHidesIdentity(t *testing.T) {
	isk, ipub := newID(t)
	_, mpub := MailboxKey([]byte("s"), 1)
	a, err := Seal(pl(10), isk, mpub)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Seal(pl(10), isk, mpub)
	if err != nil {
		t.Fatal(err)
	}
	npub, err := nip19.EncodePublicKey(ipub)
	if err != nil {
		t.Fatal(err)
	}
	j, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(j), ipub) || strings.Contains(string(j), npub) || a.PubKey == ipub {
		t.Fatal("identity leaked")
	}
	if a.PubKey == b.PubKey {
		t.Fatal("throwaway key reused")
	}
}

func TestSealFixedSize(t *testing.T) {
	isk, _ := newID(t)
	_, mpub := MailboxKey([]byte("s"), 1)
	a, err1 := Seal(pl(10), isk, mpub)
	b, err2 := Seal(pl(9000), isk, mpub)
	if err1 != nil || err2 != nil || len(a.Content) != len(b.Content) {
		t.Fatal(err1, err2)
	}
	if _, err := Seal(pl(17000), isk, mpub); err == nil {
		t.Fatal("oversize accepted")
	}
}

func TestOpenWrongMailbox(t *testing.T) {
	isk, _ := newID(t)
	_, apub := MailboxKey([]byte("a"), 1)
	bsk, bpub := MailboxKey([]byte("b"), 1)
	ev, err := Seal(pl(10), isk, apub)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Open(ev, bsk); err == nil {
		t.Fatal("wrong mailbox opened")
	}
	// replay: inner sealed for A, re-wrapped to B
	inner := sealInner(t, isk, apub, now())
	if _, _, _, err := Open(wrap(t, inner, bpub), bsk); err == nil {
		t.Fatal("replay accepted")
	}
}

func sealInner(t *testing.T, isk, mpub string, at time.Time) nostr.Event {
	t.Helper()
	c, err := json.Marshal(pl(5))
	if err != nil {
		t.Fatal(err)
	}
	in := nostr.Event{Kind: EnvelopeKind, CreatedAt: nostr.Timestamp(at.Unix()), Tags: nostr.Tags{{"m", mpub}}, Content: string(c)}
	if err := in.Sign(isk); err != nil {
		t.Fatal(err)
	}
	return in
}

func wrap(t *testing.T, in nostr.Event, mpub string) nostr.Event {
	t.Helper()
	ts := nostr.GeneratePrivateKey()
	k, err := nip44.GenerateConversationKey(mpub, ts)
	if err != nil {
		t.Fatal(err)
	}
	j, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	c, err := nip44.Encrypt(string(j)+strings.Repeat(" ", PadSize-len(j)), k)
	if err != nil {
		t.Fatal(err)
	}
	out := nostr.Event{Kind: EnvelopeKind, CreatedAt: in.CreatedAt, Tags: nostr.Tags{{"p", mpub}}, Content: c}
	if err := out.Sign(ts); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestOpenStale(t *testing.T) {
	isk, _ := newID(t)
	msk, mpub := MailboxKey([]byte("s"), 1)
	for _, c := range []struct {
		age time.Duration
		ok  bool
	}{{11 * time.Minute, false}, {9 * time.Minute, true}, {-11 * time.Minute, false}, {-9 * time.Minute, true}} {
		_, _, _, err := Open(wrap(t, sealInner(t, isk, mpub, now().Add(-c.age)), mpub), msk)
		if (err == nil) != c.ok {
			t.Fatalf("age %v: err=%v", c.age, err)
		}
	}
}

func TestMailboxRotation(t *testing.T) {
	_, a := MailboxKey([]byte("s"), 5)
	_, b := MailboxKey([]byte("s"), 5)
	_, c := MailboxKey([]byte("s"), 6)
	_, d := MailboxKey([]byte("t"), 5)
	if a != b || a == c || a == d {
		t.Fatal("rotation wrong")
	}
	if Window(time.Unix(7200, 0)) != 2 {
		t.Fatal("window")
	}
}

func TestVerifyCodeSymmetric(t *testing.T) {
	c := VerifyCode([]byte("s"), "aa", "bb")
	if c != VerifyCode([]byte("s"), "bb", "aa") || len(c) != 6 || strings.Trim(c, "0123456789") != "" {
		t.Fatal(c)
	}
}

func TestPairSecretSymmetric(t *testing.T) {
	ask, apub := newID(t)
	bsk, bpub := newID(t)
	inv := []byte("invite")
	x, err1 := PairSecret(ask, bpub, inv)
	y, err2 := PairSecret(bsk, apub, inv)
	if err1 != nil || err2 != nil || len(x) != 32 || string(x) != string(y) {
		t.Fatal("differ", err1, err2)
	}
	if _, err := PairSecret(ask, "zz", inv); err == nil {
		t.Fatal("invalid peerPub accepted")
	}
}

func TestEmptySecretPanics(t *testing.T) {
	for _, f := range []func(){func() { MailboxKey(nil, 1) }, func() { VerifyCode(nil, "a", "b") }} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			f()
		}()
	}
}

func TestOpenTamperedInner(t *testing.T) {
	isk, _ := newID(t)
	msk, mpub := MailboxKey([]byte("s"), 1)
	in := sealInner(t, isk, mpub, now())
	in.Content = `{"type":"bye"}`
	if _, _, _, err := Open(wrap(t, in, mpub), msk); err == nil {
		t.Fatal("tampered inner accepted")
	}
}
