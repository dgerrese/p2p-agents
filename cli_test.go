package main

import (
	"bytes"
	"os"
	"os/exec"
	"p2p-agents/internal/p2p"
	"path/filepath"
	"regexp"
	"strings"
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
