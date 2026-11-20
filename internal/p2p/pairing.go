package p2p

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode"

	"github.com/nbd-wtf/go-nostr"
)

var (
	ErrWrongCode     = errors.New("wrong verification code")
	ErrNoRequest     = errors.New("no such pending pairing request")
	ErrInviteExpired = errors.New("invite expired")
)

const (
	invitePrefix = "p2pa1:"
	replyWindow  = -1 // fixed MailboxKey window for an invite's reply mailbox
	maxCodeTries = 3
	maxNameRunes = 64
	pairTimeout  = time.Hour // ponytail: fixed approval window per request, make it configurable if owners need longer
)

type PairRequest struct {
	ID       string
	PeerPub  string
	PeerName string
	Expires  time.Time
}

// ponytail: invites lost on restart, persist if owners complain
type invite struct {
	sk      string
	secret  []byte
	expires time.Time
}

// pairReq is one pairing in progress.
// Joiner B: pair_req{commit=H(nB)} -> pair_resp{nA} -> pair_reveal{nB}; inviter A checks H(nB)==commit.
// The verify code is derived from sas = H("p2pa-sas"|S|nA|nB), so nobody can grind it offline.
type pairReq struct {
	PairRequest
	secret              []byte
	listenSK, listenPub string // inviter: one-time mailbox; joiner: reply mailbox
	sendPub             string // the other side's listen mailbox
	nonce               []byte // own nonce (joiner nB, inviter nA)
	commit              []byte // inviter: joiner's commit H(nB)
	sas                 []byte // set once both nonces are known; code available then
	localOK, remoteOK   bool
	tries               int
}

type inviteCode struct {
	M string   `json:"m"`
	S []byte   `json:"s"`
	E int64    `json:"e"`
	R []string `json:"r"`
}

type pairMsg struct {
	Name   string `json:"name,omitempty"`
	Commit []byte `json:"commit,omitempty"`
	Nonce  []byte `json:"nonce,omitempty"`
	MAC    []byte `json:"mac"`
}

// pairMAC = HMAC(S, label|pub1|pub2...), binding the message type and both parties.
func pairMAC(secret []byte, label string, pubs ...string) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(label))
	for _, p := range pubs {
		h.Write([]byte("|" + p))
	}
	return h.Sum(nil)
}

func sasKey(secret, nA, nB []byte) []byte {
	h := sha256.New()
	h.Write([]byte("p2pa-sas"))
	h.Write(secret)
	h.Write(nA)
	h.Write(nB)
	return h.Sum(nil)
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b) // never fails on supported platforms (Go 1.24+ crashes instead)
	return b
}

func newRequestID() string { return hex.EncodeToString(randBytes(8)) }

// cleanName strips control/format characters and caps length; peer names are untrusted.
func cleanName(s string) string {
	var out []rune
	for _, r := range s {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			continue
		}
		if out = append(out, r); len(out) == maxNameRunes {
			break
		}
	}
	return strings.TrimSpace(string(out))
}

func (n *Node) Invite(ttl time.Duration) (string, error) {
	secret := randBytes(32)
	sk := nostr.GeneratePrivateKey()
	pub, err := nostr.GetPublicKey(sk)
	if err != nil {
		return "", err
	}
	cfg, err := n.st.Config()
	if err != nil {
		return "", err
	}
	exp := now().Add(ttl)
	b, err := json.Marshal(inviteCode{M: pub, S: secret, E: exp.Unix(), R: cfg.Relays})
	if err != nil {
		return "", err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.invites[pub] = &invite{sk: sk, secret: secret, expires: exp}
	if err := n.resubscribeLocked(); err != nil {
		return "", err
	}
	return invitePrefix + base64.RawURLEncoding.EncodeToString(b), nil
}

func (n *Node) Join(code string) (string, error) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(code), invitePrefix)
	if !ok {
		return "", errors.New("not an invite code")
	}
	raw, err := base64.RawURLEncoding.DecodeString(rest)
	if err != nil {
		return "", errors.New("malformed invite code")
	}
	var c inviteCode
	if json.Unmarshal(raw, &c) != nil || len(c.S) != 32 || len(c.M) != 64 {
		return "", errors.New("malformed invite code")
	}
	if now().Unix() >= c.E {
		return "", ErrInviteExpired
	}
	nB := randBytes(32)
	commit := sha256.Sum256(nB)
	// ponytail: c.R is ignored, Join uses this node's own signals; owners with disjoint custom relays can't pair over relays until Join dials c.R too.
	ev := n.seal("pair_req", pairMsg{Name: n.name, Commit: commit[:], MAC: pairMAC(c.S, "req", n.pub)}, c.M)
	if ev == nil {
		return "", errors.New("malformed invite code")
	}
	lsk, lpub := MailboxKey(c.S, replyWindow)
	r := &pairReq{
		PairRequest: PairRequest{ID: newRequestID(), Expires: now().Add(pairTimeout)},
		secret:      c.S, listenSK: lsk, listenPub: lpub, sendPub: c.M, nonce: nB,
	}
	n.mu.Lock()
	n.reqs[r.ID] = r
	err = n.resubscribeLocked()
	n.mu.Unlock()
	if err == nil {
		err = n.publish(ev)
	}
	if err != nil {
		n.mu.Lock()
		delete(n.reqs, r.ID)
		n.resubscribeLocked()
		n.mu.Unlock()
		return "", err
	}
	return r.ID, nil
}

// Requests lists pairing requests whose verify code is available and that await approval.
func (n *Node) Requests() []PairRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []PairRequest
	for _, r := range n.reqs {
		if r.sas != nil && now().Before(r.Expires) {
			out = append(out, r.PairRequest)
		}
	}
	return out
}

// pendingLocked returns an unexpired request whose verify code is available.
func (n *Node) pendingLocked(id string) (*pairReq, error) {
	r := n.reqs[id]
	if r == nil || r.sas == nil || !now().Before(r.Expires) {
		return nil, ErrNoRequest
	}
	return r, nil
}

// VerifyCodeFor is only for TTY/elicitation paths; never put its result in events or tool output.
func (n *Node) VerifyCodeFor(id string) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, err := n.pendingLocked(id)
	if err != nil {
		return "", err
	}
	return VerifyCode(r.sas, n.pub, r.PeerPub), nil
}

func (n *Node) Accept(id, typed string) error {
	n.mu.Lock()
	r, err := n.pendingLocked(id)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	want := VerifyCode(r.sas, n.pub, r.PeerPub)
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(typed)), []byte(want)) != 1 {
		r.tries++
		if r.tries < maxCodeTries {
			n.mu.Unlock()
			return ErrWrongCode
		}
		ev := n.failLocked(r, "too many wrong codes")
		n.mu.Unlock()
		return errors.Join(ErrWrongCode, n.publish(ev))
	}
	r.localOK = true
	if r.remoteOK {
		if err := n.finishLocked(r); err != nil {
			ev := n.noLocked(r) // never confirm what we could not store
			n.mu.Unlock()
			return errors.Join(err, n.publish(ev))
		}
	}
	ev := n.seal("pair_ok", pairMsg{MAC: pairMAC(r.secret, "ok", n.pub, r.PeerPub)}, r.sendPub)
	n.mu.Unlock()
	return n.publish(ev)
}

func (n *Node) Reject(id string) error {
	n.mu.Lock()
	r := n.reqs[id]
	if r == nil {
		n.mu.Unlock()
		return ErrNoRequest
	}
	delete(n.reqs, id)
	var ev *nostr.Event
	if r.PeerPub != "" {
		ev = n.noLocked(r)
	}
	n.resubscribeLocked()
	n.mu.Unlock()
	if ev == nil {
		return nil
	}
	return n.publish(ev)
}

func (n *Node) noLocked(r *pairReq) *nostr.Event {
	return n.seal("pair_no", pairMsg{MAC: pairMAC(r.secret, "no", n.pub, r.PeerPub)}, r.sendPub)
}

// failLocked drops r, emits pair_failed and returns a pair_no for the other side.
func (n *Node) failLocked(r *pairReq, reason string) *nostr.Event {
	delete(n.reqs, r.ID)
	n.emitLocked(Event{Kind: "pair_failed", Peer: r.PeerPub, Name: r.PeerName, ID: r.ID, Reason: reason})
	n.resubscribeLocked()
	return n.noLocked(r)
}

// finishLocked stores the peer once both owners approved.
func (n *Node) finishLocked(r *pairReq) error {
	delete(n.reqs, r.ID)
	secret, err := PairSecret(n.sk, r.PeerPub, r.secret)
	if err == nil {
		err = n.st.AddPeer(Peer{Pub: r.PeerPub, Name: r.PeerName, Secret: secret, PairedAt: now()})
	}
	if err != nil {
		n.emitLocked(Event{Kind: "pair_failed", Peer: r.PeerPub, Name: r.PeerName, ID: r.ID, Reason: "could not store peer"})
		n.resubscribeLocked()
		return err
	}
	n.emitLocked(Event{Kind: "paired", Peer: r.PeerPub, Name: r.PeerName, ID: r.ID})
	return n.resubscribeLocked()
}

// handlePair processes a pairing payload that arrived at pairing mailbox mbox.
func (n *Node) handlePair(mbox string, p Payload, sender string) {
	var m pairMsg
	if json.Unmarshal(p.Data, &m) != nil {
		return
	}
	n.mu.Lock()
	var out *nostr.Event
	defer func() {
		n.mu.Unlock()
		if out != nil {
			n.publish(out)
		}
	}()
	if p.Type == "pair_req" { // inviter side; any sender that knows the secret
		inv := n.invites[mbox]
		if inv == nil || now().After(inv.expires) || len(m.Commit) != 32 || !hmac.Equal(m.MAC, pairMAC(inv.secret, "req", sender)) {
			return
		}
		delete(n.invites, mbox) // burn; the request keeps the mailbox key until done
		_, reply := MailboxKey(inv.secret, replyWindow)
		r := &pairReq{
			PairRequest: PairRequest{ID: newRequestID(), PeerPub: sender, PeerName: cleanName(m.Name), Expires: now().Add(pairTimeout)},
			secret:      inv.secret, listenSK: inv.sk, listenPub: mbox, sendPub: reply,
			nonce: randBytes(32), commit: m.Commit,
		}
		n.reqs[r.ID] = r
		out = n.seal("pair_resp", pairMsg{Name: n.name, Nonce: r.nonce, MAC: pairMAC(r.secret, "resp", n.pub, sender)}, reply)
		n.resubscribeLocked() // pair_request is emitted after a valid pair_reveal
		return
	}
	for _, r := range n.reqs {
		if r.listenPub != mbox || !now().Before(r.Expires) {
			continue
		}
		switch {
		case p.Type == "pair_resp" && r.PeerPub == "": // joiner: response must be bound to our own pub
			if len(m.Nonce) != 32 || !hmac.Equal(m.MAC, pairMAC(r.secret, "resp", sender, n.pub)) {
				continue
			}
			r.PeerPub, r.PeerName = sender, cleanName(m.Name)
			r.sas = sasKey(r.secret, m.Nonce, r.nonce)
			out = n.seal("pair_reveal", pairMsg{Nonce: r.nonce, MAC: pairMAC(r.secret, "reveal", n.pub, sender)}, r.sendPub)
			n.emitLocked(Event{Kind: "pair_request", Peer: sender, Name: r.PeerName, ID: r.ID})
			return
		case r.PeerPub != sender:
			continue
		case p.Type == "pair_reveal" && r.commit != nil && r.sas == nil: // inviter: check commit
			if !hmac.Equal(m.MAC, pairMAC(r.secret, "reveal", sender, n.pub)) {
				return
			}
			if h := sha256.Sum256(m.Nonce); !hmac.Equal(h[:], r.commit) {
				delete(n.reqs, r.ID) // fail closed, silently: the owner never saw this request
				n.resubscribeLocked()
				return
			}
			r.sas = sasKey(r.secret, r.nonce, m.Nonce)
			n.emitLocked(Event{Kind: "pair_request", Peer: sender, Name: r.PeerName, ID: r.ID})
			return
		case p.Type == "pair_ok" && r.sas != nil:
			if !hmac.Equal(m.MAC, pairMAC(r.secret, "ok", sender, n.pub)) {
				return
			}
			r.remoteOK = true
			if r.localOK {
				n.finishLocked(r)
			}
			return
		case p.Type == "pair_no":
			if !hmac.Equal(m.MAC, pairMAC(r.secret, "no", sender, n.pub)) {
				return
			}
			delete(n.reqs, r.ID)
			n.emitLocked(Event{Kind: "pair_failed", Peer: r.PeerPub, Name: r.PeerName, ID: r.ID, Reason: "rejected by peer"})
			n.resubscribeLocked()
			return
		}
	}
}
