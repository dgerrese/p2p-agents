package p2p

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type Store struct{ Dir string }

func OpenStore() (*Store, error) {
	dir := os.Getenv("P2PA_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		dir = filepath.Join(home, ".config", "p2p-agents")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Store{Dir: dir}, nil
}

// writeFile writes atomically via a unique 0600 temp file, then chmod and rename.
func writeFile(path string, data []byte, perm os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // no-op after successful rename
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Identity returns the hex secret and public key, generating them on first use.
func (s *Store) Identity() (sk, pub string, err error) {
	path := filepath.Join(s.Dir, "identity.key")
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		sk = nostr.GeneratePrivateKey()
		if err = writeFile(path, []byte(sk), 0o600); err != nil {
			return "", "", err
		}
	} else if err != nil {
		return "", "", err
	} else {
		sk = strings.TrimSpace(string(b))
	}
	pub, err = nostr.GetPublicKey(sk)
	return sk, pub, err
}

type Config struct {
	Name    string   `json:"name"`
	Relays  []string `json:"relays"`
	STUN    []string `json:"stun"`
	LANOnly bool     `json:"lan_only"`
}

func (s *Store) Config() (Config, error) {
	var c Config
	b, err := os.ReadFile(filepath.Join(s.Dir, "config.json"))
	if err == nil {
		err = json.Unmarshal(b, &c)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return c, err
	}
	if c.Name == "" {
		h, _ := os.Hostname()
		c.Name = h + " agent"
	}
	if len(c.Relays) == 0 {
		c.Relays = []string{"wss://relay.damus.io", "wss://nos.lol", "wss://relay.primal.net"}
	}
	if len(c.STUN) == 0 {
		c.STUN = []string{"stun:stun.cloudflare.com:3478", "stun:stun.l.google.com:19302"}
	}
	return c, nil
}

func (s *Store) SaveConfig(c Config) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(s.Dir, "config.json"), b, 0o644)
}

type Peer struct {
	Pub      string    `json:"pub"`
	Name     string    `json:"name"`
	Secret   []byte    `json:"secret"`
	PairedAt time.Time `json:"paired_at"`
}

func (s *Store) Peers() ([]Peer, error) {
	b, err := os.ReadFile(filepath.Join(s.Dir, "peers.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ps []Peer
	return ps, json.Unmarshal(b, &ps)
}

func (s *Store) savePeers(ps []Peer) error {
	b, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(s.Dir, "peers.json"), b, 0o600)
}

// AddPeer appends, or replaces an existing peer with the same Pub.
// ponytail: no cross-process locking, callers hold the store Lock; use flock around RMW if that changes.
func (s *Store) AddPeer(p Peer) error {
	ps, err := s.Peers()
	if err != nil {
		return err
	}
	for i := range ps {
		if ps[i].Pub == p.Pub {
			ps[i] = p
			return s.savePeers(ps)
		}
	}
	return s.savePeers(append(ps, p))
}

func (s *Store) RemovePeer(pub string) error {
	ps, err := s.Peers()
	if err != nil {
		return err
	}
	out := ps[:0]
	for _, p := range ps {
		if p.Pub != pub {
			out = append(out, p)
		}
	}
	return s.savePeers(out)
}

type LogEntry struct {
	ID      string    `json:"id"`
	Peer    string    `json:"peer"`
	Dir     string    `json:"dir"` // "in" | "out"
	Text    string    `json:"text"`
	ReplyTo string    `json:"reply_to,omitempty"`
	TS      time.Time `json:"ts"`
}

func (s *Store) logPath(peer string) (string, error) {
	if peer == "" || peer != filepath.Base(peer) || peer == "." || peer == ".." {
		return "", errors.New("invalid peer id")
	}
	return filepath.Join(s.Dir, "log", peer+".jsonl"), nil
}

func (s *Store) AppendLog(e LogEntry) error {
	path, err := s.logPath(e.Peer)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	return err
}

// ReadLog returns entries for peer with TS at or after since.
func (s *Store) ReadLog(peer string, since time.Time) ([]LogEntry, error) {
	path, err := s.logPath(peer)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []LogEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20) // entries are capped at 64 KB text, JSON-escaped fits
	for sc.Scan() {
		var e LogEntry
		if json.Unmarshal(sc.Bytes(), &e) == nil && !e.TS.Before(since) {
			out = append(out, e)
		}
	}
	return out, sc.Err()
}

// Lock takes a non-blocking exclusive flock on <dir>/lock. ok is false if held elsewhere.
func (s *Store) Lock() (release func(), ok bool, err error) {
	f, err := os.OpenFile(filepath.Join(s.Dir, "lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return func() { f.Close() }, true, nil
}
