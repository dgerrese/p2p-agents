package main

import (
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"p2p-agents/internal/p2p"
)

// Control exposes the Node over net/rpc, one method per Node method.
// ponytail: any process of the same uid can call it (incl. VerifyCodeFor/Accept); the TTY rule is a CLI guard, not a security boundary against same-uid code.
type Control struct{ n *p2p.Node }

type (
	Empty      struct{}
	InviteArgs struct{ TTL time.Duration }
	IDArgs     struct{ ID string }
	AcceptArgs struct{ ID, Typed string }
	UnpairArgs struct{ Peer, Reason string }
)

func (c *Control) Invite(a InviteArgs, r *string) (err error) { *r, err = c.n.Invite(a.TTL); return }
func (c *Control) Join(a IDArgs, r *string) (err error)       { *r, err = c.n.Join(a.ID); return } // ID carries the invite code
func (c *Control) Requests(_ Empty, r *[]p2p.PairRequest) error {
	*r = c.n.Requests()
	return nil
}
func (c *Control) VerifyCodeFor(a IDArgs, r *string) (err error) {
	*r, err = c.n.VerifyCodeFor(a.ID)
	return
}
func (c *Control) Accept(a AcceptArgs, _ *Empty) error { return c.n.Accept(a.ID, a.Typed) }
func (c *Control) Reject(a IDArgs, _ *Empty) error     { return c.n.Reject(a.ID) }
func (c *Control) Peers(_ Empty, r *[]p2p.PeerStatus) error {
	*r = c.n.Peers()
	return nil
}
func (c *Control) Unpair(a UnpairArgs, _ *Empty) error { return c.n.Unpair(a.Peer, a.Reason) }

func sockPath(st *p2p.Store) string { return filepath.Join(st.Dir, "control.sock") }

// serveControl listens on <dir>/control.sock (0600). The caller must hold the store lock,
// which is what makes removing a stale socket file safe.
func serveControl(n *p2p.Node, st *p2p.Store) (stop func(), err error) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Control", &Control{n}); err != nil {
		return nil, err
	}
	path := sockPath(st)
	os.Remove(path)
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil { // dir is 0700 too, so the window is not reachable
		l.Close()
		return nil, err
	}
	go srv.Accept(l)
	return func() { l.Close(); os.Remove(path) }, nil
}
