package p2p

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip44"
)

const (
	EnvelopeKind = 21059 // ephemeral range, relays don't store
	PadSize      = 16000
	maxSkew      = 10 * time.Minute
)

var now = time.Now // injectable for tests

// Payload Type: pair_req|pair_resp|pair_ok|pair_no|knock|offer|answer|bye
type Payload struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

func Seal(p Payload, identitySK, mailboxPub string) (nostr.Event, error) {
	content, err := json.Marshal(p)
	if err != nil {
		return nostr.Event{}, err
	}
	ts := nostr.Timestamp(now().Unix())
	inner := nostr.Event{Kind: EnvelopeKind, CreatedAt: ts, Tags: nostr.Tags{{"m", mailboxPub}}, Content: string(content)}
	if err := inner.Sign(identitySK); err != nil {
		return nostr.Event{}, err
	}
	j, err := json.Marshal(inner)
	if err != nil {
		return nostr.Event{}, err
	}
	if len(j) > PadSize {
		return nostr.Event{}, fmt.Errorf("payload too large: %d > %d", len(j), PadSize)
	}
	for len(j) < PadSize {
		j = append(j, ' ')
	}
	t := nostr.GeneratePrivateKey()
	key, err := nip44.GenerateConversationKey(mailboxPub, t)
	if err != nil {
		return nostr.Event{}, err
	}
	enc, err := nip44.Encrypt(string(j), key)
	if err != nil {
		return nostr.Event{}, err
	}
	outer := nostr.Event{Kind: EnvelopeKind, CreatedAt: ts, Tags: nostr.Tags{{"p", mailboxPub}}, Content: enc}
	return outer, outer.Sign(t)
}

// Open decrypts and verifies an envelope. Callers must dedupe on innerID (a peer
// can re-wrap a captured inner event under a new outer ID) and check that
// senderPub is the expected peer and not themselves (reflection).
func Open(ev nostr.Event, mailboxSK string) (p Payload, senderPub, innerID string, err error) {
	if ok, _ := ev.CheckSignature(); !ok {
		return p, "", "", errors.New("bad outer signature")
	}
	mpub, err := nostr.GetPublicKey(mailboxSK)
	if err != nil {
		return p, "", "", err
	}
	key, err := nip44.GenerateConversationKey(ev.PubKey, mailboxSK)
	if err != nil {
		return p, "", "", err
	}
	plain, err := nip44.Decrypt(ev.Content, key)
	if err != nil {
		return p, "", "", err
	}
	var inner nostr.Event
	if err := json.Unmarshal([]byte(plain), &inner); err != nil { // trailing padding spaces are fine
		return p, "", "", err
	}
	if ok, _ := inner.CheckSignature(); !ok {
		return p, "", "", errors.New("bad inner signature")
	}
	if inner.Kind != EnvelopeKind {
		return p, "", "", errors.New("bad inner kind")
	}
	if m := inner.Tags.Find("m"); len(m) < 2 || m[1] != mpub {
		return p, "", "", errors.New("inner mailbox mismatch")
	}
	if d := now().Sub(inner.CreatedAt.Time()); d > maxSkew || d < -maxSkew {
		return p, "", "", errors.New("inner event stale or from the future")
	}
	if err := json.Unmarshal([]byte(inner.Content), &p); err != nil {
		return p, "", "", err
	}
	return p, inner.PubKey, inner.ID, nil
}

// MailboxKey derives the rotating mailbox keypair for a pairing secret and hour window.
func MailboxKey(secret []byte, window int64) (sk, pub string) {
	if len(secret) == 0 {
		panic("MailboxKey: empty secret")
	}
	info := "p2pa-mailbox:" + strconv.FormatInt(window, 10)
	for i := 0; ; i++ {
		if i > 0 {
			info += ":" + strconv.Itoa(i) // re-derive until a valid scalar
		}
		b, _ := hkdf.Key(sha256.New, secret, nil, info, 32)
		var s secp256k1.ModNScalar
		if overflow := s.SetByteSlice(b); !overflow && !s.IsZero() {
			sk = hex.EncodeToString(b)
			pub, _ = nostr.GetPublicKey(sk)
			return
		}
	}
}

func Window(t time.Time) int64 { return t.Unix() / 3600 }

func VerifyCode(secret []byte, pubA, pubB string) string {
	if len(secret) == 0 {
		panic("VerifyCode: empty secret")
	}
	if pubA > pubB {
		pubA, pubB = pubB, pubA
	}
	h := sha256.New()
	h.Write(secret)
	h.Write([]byte(pubA))
	h.Write([]byte(pubB))
	return fmt.Sprintf("%06d", binary.BigEndian.Uint32(h.Sum(nil)[:4])%1_000_000)
}

func PairSecret(identitySK, peerPub string, inviteSecret []byte) ([]byte, error) {
	ck, err := nip44.GenerateConversationKey(peerPub, identitySK)
	if err != nil {
		return nil, err
	}
	return hkdf.Key(sha256.New, append(ck[:], inviteSecret...), nil, "p2pa-pair", 32)
}
