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

type pairReq struct {
	PairRequest
	secret              []byte
	listenSK, listenPub string // inviter: one-time mailbox; joiner: reply mailbox
	sendPub             string // the other side's listen mailbox
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
	Name string `json:"name,omitempty"`
	HMAC []byte `json:"hmac,omitempty"`
}

func pairMAC(secret []byte, pub string) []byte {
	h := hmac.New(sha256.New, secret)
	h.Write([]byte(pub))
	return h.Sum(nil)
}

func newRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (n *Node) Invite(ttl time.Duration) (string, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
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
	// ponytail: c.R is ignored, Join uses this node's own signals; owners with disjoint custom relays can't pair over relays until Join dials c.R too.
	ev := n.seal("pair_req", pairMsg{Name: n.name, HMAC: pairMAC(c.S, n.pub)}, c.M)
	if ev == nil {
		return "", errors.New("malformed invite code")
	}
	lsk, lpub := MailboxKey(c.S, replyWindow)
	r := &pairReq{
		PairRequest: PairRequest{ID: newRequestID(), Expires: now().Add(pairTimeout)},
		secret:      c.S, listenSK: lsk, listenPub: lpub, sendPub: c.M,
	}
	n.mu.Lock()
	n.reqs[r.ID] = r
	err = n.resubscribeLocked()
	n.mu.Unlock()
	if err != nil {
		return "", err
	}
	return r.ID, n.publish(ev)
}

// Requests lists pairing requests whose peer is known and that await approval.
func (n *Node) Requests() []PairRequest {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []PairRequest
	for _, r := range n.reqs {
		if r.PeerPub != "" && now().Before(r.Expires) {
			out = append(out, r.PairRequest)
		}
	}
	return out
}

// pendingLocked returns an unexpired request with a known peer.
func (n *Node) pendingLocked(id string) (*pairReq, error) {
	r := n.reqs[id]
	if r == nil || r.PeerPub == "" || !now().Before(r.Expires) {
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
	return VerifyCode(r.secret, n.pub, r.PeerPub), nil
}

func (n *Node) Accept(id, typed string) error {
	n.mu.Lock()
	r, err := n.pendingLocked(id)
	if err != nil {
		n.mu.Unlock()
		return err
	}
	want := VerifyCode(r.secret, n.pub, r.PeerPub)
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(typed)), []byte(want)) != 1 {
		r.tries++
		var ev *nostr.Event
		if r.tries >= maxCodeTries {
			ev = n.failLocked(r, "too many wrong codes")
		}
		n.mu.Unlock()
		if ev != nil {
			n.publish(ev)
		}
		return ErrWrongCode
	}
	r.localOK = true
	ev := n.seal("pair_ok", pairMsg{}, r.sendPub)
	if r.remoteOK {
		err = n.finishLocked(r)
	}
	n.mu.Unlock()
	n.publish(ev)
	return err
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
		ev = n.seal("pair_no", pairMsg{}, r.sendPub)
	}
	n.resubscribeLocked()
	n.mu.Unlock()
	if ev != nil {
		n.publish(ev)
	}
	return nil
}

// failLocked drops r, emits pair_failed and returns a pair_no for the other side.
func (n *Node) failLocked(r *pairReq, reason string) *nostr.Event {
	delete(n.reqs, r.ID)
	n.emitLocked(Event{Kind: "pair_failed", Peer: r.PeerPub, Name: r.PeerName, ID: r.ID, Reason: reason})
	n.resubscribeLocked()
	return n.seal("pair_no", pairMsg{}, r.sendPub)
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
		if inv == nil || now().After(inv.expires) || !hmac.Equal(m.HMAC, pairMAC(inv.secret, sender)) {
			return
		}
		delete(n.invites, mbox) // burn; the request keeps the mailbox key until done
		_, reply := MailboxKey(inv.secret, replyWindow)
		r := &pairReq{
			PairRequest: PairRequest{ID: newRequestID(), PeerPub: sender, PeerName: m.Name, Expires: now().Add(pairTimeout)},
			secret:      inv.secret, listenSK: inv.sk, listenPub: mbox, sendPub: reply,
		}
		n.reqs[r.ID] = r
		out = n.seal("pair_resp", pairMsg{Name: n.name, HMAC: pairMAC(inv.secret, n.pub)}, reply)
		n.emitLocked(Event{Kind: "pair_request", Peer: sender, Name: m.Name, ID: r.ID})
		n.resubscribeLocked()
		return
	}
	for _, r := range n.reqs {
		if r.listenPub != mbox {
			continue
		}
		switch {
		case p.Type == "pair_resp" && r.PeerPub == "": // joiner side, first response establishes the peer
			if !hmac.Equal(m.HMAC, pairMAC(r.secret, sender)) {
				continue
			}
			r.PeerPub, r.PeerName = sender, m.Name
			n.emitLocked(Event{Kind: "pair_request", Peer: sender, Name: m.Name, ID: r.ID})
			return
		case r.PeerPub != sender:
			continue
		case p.Type == "pair_ok":
			r.remoteOK = true
			if r.localOK {
				n.finishLocked(r)
			}
			return
		case p.Type == "pair_no":
			delete(n.reqs, r.ID)
			n.emitLocked(Event{Kind: "pair_failed", Peer: r.PeerPub, Name: r.PeerName, ID: r.ID, Reason: "rejected by peer"})
			n.resubscribeLocked()
			return
		}
	}
}
