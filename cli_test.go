package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"p2p-agents/internal/p2p"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

var sixDigits = regexp.MustCompile(`\b\d{6}\b`)

// setup builds the binary and a temp home that never touches the LAN or real relays.
func setup(t *testing.T) (bin, home string) {
	t.Helper()
	dir := t.TempDir()
	bin = filepath.Join(dir, "p2p-agents")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	home = filepath.Join(dir, "home")
	os.MkdirAll(home, 0o700)
	// unreachable local relay: the node needs one signal but nothing leaves the machine
	os.WriteFile(filepath.Join(home, "config.json"), []byte(`{"name":"t","relays":["ws://127.0.0.1:1"]}`), 0o644)
	return
}

func run(t *testing.T, bin, home string, args ...string) (stdout, stderr string, err error) {
	var o, e bytes.Buffer
	c := exec.Command(bin, args...)
	c.Env = append(os.Environ(), "P2PA_HOME="+home, "P2PA_NO_LAN=1")
	c.Stdout, c.Stderr = &o, &e // pipes, not a TTY
	err = c.Run()
	return o.String(), e.String(), err
}

func TestAcceptRefusesWithoutTTY(t *testing.T) {
	bin, home := setup(t)
	out, errOut, err := run(t, bin, home, "accept", "abcd")
	if err == nil {
		t.Fatal("accept succeeded without a TTY")
	}
	if !strings.Contains(errOut, "interactive terminal") {
		t.Fatalf("stderr = %q", errOut)
	}
	if sixDigits.MatchString(out) {
		t.Fatalf("stdout leaked a code: %q", out)
	}
}

func TestRequestsHidesCode(t *testing.T) {
	bin, home := setup(t)
	out, errOut, err := run(t, bin, home, "requests")
	if err != nil {
		t.Fatalf("%v: %s", err, errOut)
	}
	if sixDigits.MatchString(out) {
		t.Fatalf("requests printed a code: %q", out)
	}
}

func TestCLIUsesRunningInstance(t *testing.T) {
	bin, home := setup(t)
	c := exec.Command(bin, "invite")
	c.Env = append(os.Environ(), "P2PA_HOME="+home, "P2PA_NO_LAN=1")
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { c.Process.Kill(); c.Wait() }()
	sock := filepath.Join(home, "control.sock")
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if fi, err := os.Stat(sock); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket: %v %v", fi, err)
	}
	out, errOut, err := run(t, bin, home, "peers")
	if err != nil {
		t.Fatalf("peers via socket: %v: %s", err, errOut)
	}
	if !strings.Contains(out, "No paired peers") {
		t.Fatalf("out = %q", out)
	}
}

func TestResolvePeer(t *testing.T) {
	ps := []p2p.PeerStatus{{Pub: "aaaaaaaa11", Name: "bob"}, {Pub: "aaaaaaaa22", Name: "al"}, {Pub: "cccccccc33", Name: "bob"}}
	if p, err := resolvePeer(ps, "aaaaaaaa11"); err != nil || p.Name != "bob" {
		t.Fatal(p, err)
	}
	if _, err := resolvePeer(ps, "bob"); err == nil || !strings.Contains(err.Error(), "cccccccc33") {
		t.Fatal("want ambiguity listing", err)
	}
	if _, err := resolvePeer(ps, "aaaa"); err == nil { // < 8 chars never matches a prefix
		t.Fatal("short prefix matched")
	}
}

// fakeAPI is one pending request that shows 654321, expects the other side's 123456, and has a node-like 3-try limit.
type fakeAPI struct {
	NodeAPI
	mu                 sync.Mutex
	wrong              int
	accepted, rejected bool
	peers              []p2p.PeerStatus
	gone, noPair       bool
}

const fakePub = "aaaaaaaabbbbbbbbccccccccdddddddd"

func (f *fakeAPI) Requests() ([]p2p.PairRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gone {
		return nil, nil
	}
	return []p2p.PairRequest{{ID: "r1", PeerPub: fakePub, PeerName: "bob"}}, nil
}
func (f *fakeAPI) VerifyCodeFor(string) (string, error) { return "654321", nil }
func (f *fakeAPI) Peers() ([]p2p.PeerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peers, nil
}
func (f *fakeAPI) Reject(string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejected = true
	return nil
}
func (f *fakeAPI) Accept(_, typed string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gone {
		return p2p.ErrNoRequest
	}
	if typed == "123456" {
		f.accepted = true
		if !f.noPair {
			f.peers = []p2p.PeerStatus{{Pub: fakePub, Name: "bob"}}
		}
		return nil
	}
	if f.wrong++; f.wrong >= 3 {
		f.gone = true
	}
	return p2p.ErrWrongCode
}

func prompt(t *testing.T, f *fakeAPI, input string) (string, error) {
	t.Helper()
	isTTY = func() bool { return true }
	t.Cleanup(func() { isTTY = func() bool { return false } })
	var out bytes.Buffer
	err := promptAccept(context.Background(), f, "r1", &out, bufio.NewReader(strings.NewReader(input)))
	return out.String(), err
}

func TestPromptEnterRejects(t *testing.T) {
	f := &fakeAPI{}
	out, err := prompt(t, f, "\n")
	if err != nil || !f.rejected || f.accepted || !strings.Contains(out, "Read this code to the other owner: 654321\nType the code the other owner reads to you to accept, or press Enter to reject: ") {
		t.Fatal(err, f, out)
	}
}

func TestPromptWrongThenRight(t *testing.T) {
	f := &fakeAPI{}
	out, err := prompt(t, f, "000000\n123456\n")
	if err != nil || !f.accepted || strings.Count(out, "Wrong code.") != 1 {
		t.Fatal(err, f, out)
	}
}

func TestPromptThirdWrongFails(t *testing.T) {
	f := &fakeAPI{}
	out, err := prompt(t, f, "1\n2\n3\n123456\n")
	if err == nil || f.accepted || !strings.Contains(err.Error(), "too many wrong codes") || strings.Count(out, "Wrong code.") != 2 {
		t.Fatal(err, f, out)
	}
}

func TestPromptRefusesWithoutTTY(t *testing.T) {
	isTTY = func() bool { return false }
	f := &fakeAPI{}
	var out bytes.Buffer
	err := promptAccept(context.Background(), f, "r1", &out, bufio.NewReader(strings.NewReader("123456\n")))
	if err == nil || f.accepted || out.Len() != 0 {
		t.Fatal(err, out.String())
	}
}

func TestPromptInterruptDoesNotReject(t *testing.T) {
	f := &fakeAPI{}
	isTTY = func() bool { return true }
	defer func() { isTTY = func() bool { return false } }()
	ctx, cancel := context.WithCancel(context.Background())
	pr, _ := io.Pipe() // never delivers input
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	err := promptAccept(ctx, f, "r1", io.Discard, bufio.NewReader(pr))
	if err == nil || !strings.Contains(err.Error(), "interrupted") || f.rejected {
		t.Fatal(err, f)
	}
}

func TestPromptSurfacesRemoteRejection(t *testing.T) {
	f := &fakeAPI{noPair: true}
	done := make(chan error, 1)
	go func() { _, err := prompt(t, f, "123456\n"); done <- err }()
	time.Sleep(100 * time.Millisecond)
	f.mu.Lock()
	f.gone = true // pair_no arrived: request gone, peer never added
	f.mu.Unlock()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "rejected") {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("still waiting after rejection")
	}
}

func TestRequestsHidesRealCode(t *testing.T) {
	var out bytes.Buffer
	if err := printRequests(&fakeAPI{}, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "123456") || strings.Contains(out.String(), "654321") || !strings.Contains(out.String(), "r1") {
		t.Fatal(out.String())
	}
}

// TestListingsDoNotStartNode: with no instance running, peers/requests read the store.
// The config has no usable transport, so building a Node would fail; the lock stays free.
func TestListingsDoNotStartNode(t *testing.T) {
	home := t.TempDir()
	t.Setenv("P2PA_HOME", home)
	t.Setenv("P2PA_NO_LAN", "1")
	os.WriteFile(filepath.Join(home, "config.json"), []byte(`{"lan_only":true}`), 0o600)
	st := &p2p.Store{Dir: home}
	if err := st.AddPeer(p2p.Peer{Pub: strings.Repeat("ab", 32), Name: "bob", Secret: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	for cmd, want := range map[string]string{"peers": `"bob"  abababab`, "requests": "No pending pairing requests."} {
		var out bytes.Buffer
		if err := runCLI(context.Background(), []string{cmd}, strings.NewReader(""), &out); err != nil || !strings.Contains(out.String(), want) {
			t.Fatalf("%s: %v %q", cmd, err, out.String())
		}
	}
	release, ok, err := st.Lock()
	if err != nil || !ok {
		t.Fatalf("lock not free afterwards: ok=%v err=%v", ok, err)
	}
	release()
}

func TestNameCapped(t *testing.T) {
	t.Setenv("P2PA_HOME", t.TempDir())
	var out bytes.Buffer
	if err := runCLI(context.Background(), []string{"name", strings.Repeat("é", 65)}, nil, &out); err == nil {
		t.Fatal("65-rune name accepted")
	}
	if err := runCLI(context.Background(), []string{"name", strings.Repeat("é", 64)}, nil, &out); err != nil {
		t.Fatal(err)
	}
}
