package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/rpc"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/term"

	"p2p-agents/internal/p2p"
)

// NodeAPI is the slice of the Node the CLI needs; local wraps *p2p.Node, remote dials the control socket.
type NodeAPI interface {
	Invite(ttl time.Duration) (string, error)
	Join(code string) (string, error)
	Requests() ([]p2p.PairRequest, error)
	VerifyCodeFor(id string) (string, error)
	Accept(id, typed string) error
	Reject(id string) error
	Peers() ([]p2p.PeerStatus, error)
	Unpair(peerPub, reason string) error
}

type local struct{ n *p2p.Node }

func (l local) Invite(t time.Duration) (string, error)  { return l.n.Invite(t) }
func (l local) Join(c string) (string, error)           { return l.n.Join(c) }
func (l local) Requests() ([]p2p.PairRequest, error)    { return l.n.Requests(), nil }
func (l local) VerifyCodeFor(id string) (string, error) { return l.n.VerifyCodeFor(id) }
func (l local) Accept(id, typed string) error           { return l.n.Accept(id, typed) }
func (l local) Reject(id string) error                  { return l.n.Reject(id) }
func (l local) Peers() ([]p2p.PeerStatus, error)        { return l.n.Peers(), nil }
func (l local) Unpair(p, r string) error                { return l.n.Unpair(p, r) }

// offline answers listings from disk when no instance runs: nothing can be pending then.
// Only Peers and Requests are implemented.
type offline struct {
	NodeAPI
	st *p2p.Store
}

func (o offline) Requests() ([]p2p.PairRequest, error) { return nil, nil }
func (o offline) Peers() ([]p2p.PeerStatus, error)     { return storePeers(o.st) }

const maxNameRunes = 64 // same cap peers apply to names they receive

type remote struct{ c *rpc.Client }

func (r remote) Invite(t time.Duration) (s string, err error) {
	err = r.c.Call("Control.Invite", InviteArgs{t}, &s)
	return
}
func (r remote) Join(code string) (s string, err error) {
	err = r.c.Call("Control.Join", IDArgs{code}, &s)
	return
}
func (r remote) Requests() (o []p2p.PairRequest, err error) {
	err = r.c.Call("Control.Requests", Empty{}, &o)
	return
}
func (r remote) VerifyCodeFor(id string) (s string, err error) {
	err = r.c.Call("Control.VerifyCodeFor", IDArgs{id}, &s)
	return
}
func (r remote) Accept(id, typed string) error {
	return r.c.Call("Control.Accept", AcceptArgs{id, typed}, &Empty{})
}
func (r remote) Reject(id string) error { return r.c.Call("Control.Reject", IDArgs{id}, &Empty{}) }
func (r remote) Peers() (o []p2p.PeerStatus, err error) {
	err = r.c.Call("Control.Peers", Empty{}, &o)
	return
}
func (r remote) Unpair(p, reason string) error {
	return r.c.Call("Control.Unpair", UnpairArgs{p, reason}, &Empty{})
}

// nodeClient runs the Node in-process if we get the lock (and serves the control socket
// for the command's duration); otherwise it dials the instance that holds the lock.
func nodeClient(st *p2p.Store) (api NodeAPI, cleanup func(), err error) {
	release, ok, err := st.Lock()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		var c *rpc.Client
		for i := 0; i < 20; i++ { // the holder may not have created the socket yet
			if c, err = rpc.Dial("unix", sockPath(st)); err == nil {
				return remote{c}, func() { c.Close() }, nil
			}
			time.Sleep(100 * time.Millisecond)
		}
		return nil, nil, fmt.Errorf("another p2p-agents instance holds the lock but its control socket is unreachable: %w", err)
	}
	n, err := p2p.NewNodeFromConfig(st)
	if err != nil {
		release()
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { n.Run(ctx); close(done) }()
	stop, err := serveControl(n, st)
	if err != nil {
		cancel()
		<-done
		release()
		return nil, nil, err
	}
	return local{n}, func() { stop(); cancel(); <-done; release() }, nil
}

var isTTY = func() bool { return term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd())) }

func requireTTY() error {
	if isTTY() {
		return nil
	}
	return errors.New("accept must be run by the owner in an interactive terminal")
}

func runCLI(ctx context.Context, args []string, in io.Reader, out io.Writer) error {
	st, err := p2p.OpenStore()
	if err != nil {
		return err
	}
	cmd, rest := args[0], args[1:]
	need := func(n int, usage string) error {
		if len(rest) < n {
			return errors.New("usage: p2p-agents " + cmd + " " + usage)
		}
		return nil
	}
	switch cmd {
	case "whoami":
		_, pub, err := st.Identity()
		if err != nil {
			return err
		}
		cfg, err := st.Config()
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "name: %q\nfingerprint: %s\npub: %s\n", cfg.Name, p2p.Fingerprint(pub), pub)
		return nil
	case "name":
		if err := need(1, "<name>"); err != nil {
			return err
		}
		cfg, err := st.Config()
		if err != nil {
			return err
		}
		cfg.Name = strings.Join(rest, " ")
		if utf8.RuneCountInString(cfg.Name) > maxNameRunes {
			return fmt.Errorf("name too long (max %d characters)", maxNameRunes)
		}
		fmt.Fprintln(out, "Name saved; a running instance picks it up on restart.")
		return st.SaveConfig(cfg)
	case "accept":
		if err := requireTTY(); err != nil { // before anything else, even before touching the node
			return err
		}
		if err := need(1, "<id>"); err != nil {
			return err
		}
	case "pair":
		if err := need(1, "<code>"); err != nil {
			return err
		}
		if err := requireTTY(); err != nil {
			return err
		}
	case "requests", "invite", "reject", "peers", "unpair":
	default:
		return fmt.Errorf("unknown command %q (whoami, name, invite, pair, requests, accept, reject, peers, unpair)", cmd)
	}
	if (cmd == "reject" || cmd == "unpair") && len(rest) < 1 {
		return errors.New("usage: p2p-agents " + cmd + " <id|peer>")
	}

	var api NodeAPI
	if cmd == "peers" || cmd == "requests" {
		// A listing never starts a Node: that would touch the network and briefly hold the lock.
		// ponytail: an instance that holds the lock but has no socket yet gets the store view.
		if c, err := rpc.Dial("unix", sockPath(st)); err == nil {
			defer c.Close()
			api = remote{c}
		} else {
			api = offline{st: st}
		}
	} else {
		var cleanup func()
		if api, cleanup, err = nodeClient(st); err != nil {
			return err
		}
		defer cleanup()
	}
	_, inProc := api.(local)
	r := bufio.NewReader(in)

	switch cmd {
	case "invite":
		code, err := api.Invite(24 * time.Hour)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "Invite code (valid 24h; share it with the other owner):\n%s\n", code)
		if !inProc {
			fmt.Fprintln(out, "Another instance is running; use `p2p-agents requests` and `accept <id>` when the other owner joins.")
			return nil
		}
		return inviteWait(ctx, api, out, r)
	case "pair":
		id, err := api.Join(rest[0])
		if err != nil {
			return err
		}
		fmt.Fprintln(out, "Waiting for the other owner...")
		return waitAndPrompt(ctx, api, id, out, r)
	case "requests":
		return printRequests(api, out)
	case "accept":
		return promptAccept(ctx, api, rest[0], out, r)
	case "reject":
		if err := api.Reject(rest[0]); err != nil {
			return err
		}
		fmt.Fprintln(out, "Rejected.")
		return nil
	case "peers":
		ps, err := api.Peers()
		if err != nil {
			return err
		}
		if len(ps) == 0 {
			fmt.Fprintln(out, "No paired peers.")
		}
		for _, p := range ps {
			state := "offline"
			if p.Online {
				state = "online"
			}
			fmt.Fprintf(out, "%q  %s  %s  %s\n", p.Name, p.Pub[:8], p.Fingerprint, state)
		}
		return nil
	case "unpair":
		ps, err := api.Peers()
		if err != nil {
			return err
		}
		p, err := resolvePeer(ps, rest[0])
		if err != nil {
			return err
		}
		if err := api.Unpair(p.Pub, strings.Join(rest[1:], " ")); err != nil {
			return err
		}
		fmt.Fprintf(out, "Unpaired %q.\n", p.Name)
		return nil
	}
	return nil
}

// printRequests never prints the verify code.
func printRequests(api NodeAPI, out io.Writer) error {
	reqs, err := api.Requests()
	if err != nil {
		return err
	}
	if len(reqs) == 0 {
		fmt.Fprintln(out, "No pending pairing requests.")
	}
	for _, q := range reqs {
		fmt.Fprintf(out, "%s  %q  (fingerprint %s)\n", q.ID, q.PeerName, p2p.Fingerprint(q.PeerPub))
	}
	return nil
}

// resolvePeer matches a name, a pub hex prefix of >= 8 chars, or a full pub.
func resolvePeer(ps []p2p.PeerStatus, q string) (p2p.PeerStatus, error) {
	var m []p2p.PeerStatus
	for _, p := range ps {
		if p.Name == q || (len(q) >= 8 && strings.HasPrefix(p.Pub, strings.ToLower(q))) {
			m = append(m, p)
		}
	}
	switch len(m) {
	case 0:
		return p2p.PeerStatus{}, fmt.Errorf("no peer matches %q", q)
	case 1:
		return m[0], nil
	}
	var b strings.Builder
	for _, p := range m {
		fmt.Fprintf(&b, "\n  \"%s\"  %s", quoteName(p.Name), p.Pub) // reaches the model via MCP tools
	}
	return p2p.PeerStatus{}, fmt.Errorf("%q matches several peers:%s", q, b.String())
}

const pollEvery = 300 * time.Millisecond

// ponytail: polling with a fixed 10 min cap; a peer that rejects or vanishes is only noticed at the cap. Use Events() in-process if that hurts.
const waitCap = 10 * time.Minute

// poll runs f until it reports done or errors; it returns ctx.Err() on interrupt and errTimeout at the cap.
func poll(ctx context.Context, f func() (bool, error)) error {
	t := time.NewTimer(waitCap)
	defer t.Stop()
	for {
		if done, err := f(); done || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return errTimeout
		case <-time.After(pollEvery):
		}
	}
}

var errTimeout = errors.New("timed out waiting for the other owner")

func peerCount(api NodeAPI) int { ps, _ := api.Peers(); return len(ps) }

// inviteWait serves incoming requests until a pairing completes (in this or another terminal).
func inviteWait(ctx context.Context, api NodeAPI, out io.Writer, r *bufio.Reader) error {
	before := peerCount(api)
	tty := requireTTY() == nil
	seen := map[string]bool{}
	fmt.Fprintln(out, "Waiting for the other owner to join (Ctrl-C to stop)...")
	for ctx.Err() == nil {
		if peerCount(api) > before {
			fmt.Fprintln(out, "Paired.")
			return nil
		}
		reqs, _ := api.Requests()
		for _, q := range reqs {
			if seen[q.ID] {
				continue
			}
			seen[q.ID] = true
			if !tty {
				fmt.Fprintf(out, "Pairing request %s from %q (fingerprint %s); run `p2p-agents accept %s` in a terminal.\n", q.ID, q.PeerName, p2p.Fingerprint(q.PeerPub), q.ID)
				continue
			}
			if err := promptAccept(ctx, api, q.ID, out, r); err != nil {
				fmt.Fprintln(out, err)
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(pollEvery):
		}
	}
	return ctx.Err()
}

// waitAndPrompt waits for the verify code of request id (joiner side), then prompts.
func waitAndPrompt(ctx context.Context, api NodeAPI, id string, out io.Writer, r *bufio.Reader) error {
	if err := poll(ctx, func() (bool, error) { _, err := api.VerifyCodeFor(id); return err == nil, nil }); err != nil {
		return err
	}
	return promptAccept(ctx, api, id, out, r)
}

// readLine reads a line without blocking past ctx (the reader goroutine is abandoned on interrupt).
func readLine(ctx context.Context, r *bufio.Reader) (string, error) {
	type res struct {
		s   string
		err error
	}
	c := make(chan res, 1)
	go func() { s, err := r.ReadString('\n'); c <- res{s, err} }()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case x := <-c:
		return x.s, x.err
	}
}

// promptAccept shows the code and takes the typed code. The TTY check is repeated here
// so no path to Accept can skip it. Enter rejects; 3 wrong codes fail the request.
func promptAccept(ctx context.Context, api NodeAPI, id string, out io.Writer, r *bufio.Reader) error {
	if err := requireTTY(); err != nil {
		return err
	}
	var req *p2p.PairRequest
	reqs, err := api.Requests()
	if err != nil {
		return err
	}
	for i := range reqs {
		if reqs[i].ID == id {
			req = &reqs[i]
		}
	}
	code, err := api.VerifyCodeFor(id)
	if err != nil || req == nil {
		return errors.New("no such pending pairing request")
	}
	for tries := 0; tries < 3; tries++ {
		fmt.Fprintf(out, "Pairing request from %q (fingerprint %s).\nCompare this code with the other owner: %s\nType the code to accept, or press Enter to reject: ", req.PeerName, p2p.Fingerprint(req.PeerPub), code)
		line, rerr := readLine(ctx, r)
		if ctx.Err() != nil {
			return errors.New("interrupted")
		}
		line = strings.TrimSpace(line)
		if line == "" { // Enter, or EOF
			if rerr == nil || rerr == io.EOF {
				if err := api.Reject(id); err != nil {
					return err
				}
				fmt.Fprintln(out, "\nRejected.")
				return nil
			}
			return rerr
		}
		err := api.Accept(id, line)
		if err == nil {
			fmt.Fprintln(out, "Accepted. Waiting for the other owner to accept...")
			if err := poll(ctx, func() (bool, error) {
				if paired(api, req.PeerPub) {
					return true, nil
				}
				reqs, err := api.Requests()
				if err != nil {
					return false, err
				}
				for _, q := range reqs {
					if q.ID == id {
						return false, nil
					}
				}
				if paired(api, req.PeerPub) { // finished between the two calls
					return true, nil
				}
				return false, errors.New("the other owner rejected the pairing (or it failed)")
			}); err != nil {
				return err
			}
			fmt.Fprintf(out, "Paired with %q.\n", req.PeerName)
			return nil
		}
		if tries > 0 && strings.Contains(err.Error(), p2p.ErrNoRequest.Error()) { // node already failed it
			return errors.New("pairing failed (too many wrong codes or expired)")
		}
		if !strings.Contains(err.Error(), p2p.ErrWrongCode.Error()) {
			return err
		}
		if tries == 2 {
			return errors.New("too many wrong codes; pairing failed")
		}
		fmt.Fprintln(out, "Wrong code.")
	}
	return nil
}

func paired(api NodeAPI, pub string) bool {
	ps, _ := api.Peers()
	for _, p := range ps {
		if p.Pub == pub {
			return true
		}
	}
	return false
}
