package p2p

import (
	"os"
	"testing"
	"time"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	t.Setenv("P2PA_HOME", t.TempDir())
	s, err := OpenStore()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func mode(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return uint32(fi.Mode().Perm())
}

func TestIdentityStable(t *testing.T) {
	s := newStore(t)
	sk1, pub1, err := s.Identity()
	if err != nil || len(sk1) != 64 || len(pub1) != 64 {
		t.Fatal(sk1, pub1, err)
	}
	sk2, pub2, _ := s.Identity()
	if sk1 != sk2 || pub1 != pub2 {
		t.Fatal("identity changed")
	}
	if m := mode(t, s.Dir+"/identity.key"); m != 0o600 {
		t.Fatalf("mode %o", m)
	}
}

func TestPeersRoundTrip(t *testing.T) {
	s := newStore(t)
	if err := s.AddPeer(Peer{Pub: "aa", Name: "A", Secret: []byte{1, 2}, PairedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	s.AddPeer(Peer{Pub: "bb", Name: "B"})
	ps, _ := s.Peers()
	if len(ps) != 2 || ps[0].Name != "A" || string(ps[0].Secret) != "\x01\x02" {
		t.Fatalf("%+v", ps)
	}
	if m := mode(t, s.Dir+"/peers.json"); m != 0o600 {
		t.Fatalf("mode %o", m)
	}
	if err := s.RemovePeer("aa"); err != nil {
		t.Fatal(err)
	}
	if ps, _ = s.Peers(); len(ps) != 1 || ps[0].Pub != "bb" {
		t.Fatalf("%+v", ps)
	}
}

func TestConfigDefaults(t *testing.T) {
	s := newStore(t)
	c, err := s.Config()
	if err != nil || len(c.Relays) != 3 || len(c.STUN) != 2 || c.LANOnly || c.Name == "" {
		t.Fatalf("%+v %v", c, err)
	}
	c.Name, c.LANOnly = "me", true
	if err := s.SaveConfig(c); err != nil {
		t.Fatal(err)
	}
	if c2, _ := s.Config(); c2.Name != "me" || !c2.LANOnly || len(c2.Relays) != 3 {
		t.Fatalf("%+v", c2)
	}
}

func TestLockExclusive(t *testing.T) {
	s := newStore(t)
	rel, ok, err := s.Lock()
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if _, ok, _ := s.Lock(); ok {
		t.Fatal("second lock succeeded")
	}
	rel()
	rel2, ok, _ := s.Lock()
	if !ok {
		t.Fatal("lock not reacquired")
	}
	rel2()
}

func TestLogSince(t *testing.T) {
	s := newStore(t)
	t0 := time.Now().Add(-time.Hour)
	s.AppendLog(LogEntry{ID: "1", Peer: "aa", Dir: "in", Text: "old", TS: t0})
	s.AppendLog(LogEntry{ID: "2", Peer: "aa", Dir: "out", Text: "new", ReplyTo: "1", TS: time.Now()})
	s.AppendLog(LogEntry{ID: "3", Peer: "bb", Dir: "in", Text: "other", TS: time.Now()})
	es, err := s.ReadLog("aa", t0.Add(time.Minute))
	if err != nil || len(es) != 1 || es[0].ID != "2" || es[0].ReplyTo != "1" {
		t.Fatalf("%+v %v", es, err)
	}
	if es, _ = s.ReadLog("aa", time.Time{}); len(es) != 2 {
		t.Fatalf("%+v", es)
	}
	if es, err = s.ReadLog("zz", time.Time{}); err != nil || len(es) != 0 {
		t.Fatal(es, err)
	}
}
