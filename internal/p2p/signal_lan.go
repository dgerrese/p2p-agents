package p2p

import (
	"context"
	"encoding/json"
	"net"

	"github.com/nbd-wtf/go-nostr"
	"golang.org/x/net/ipv4"
)

var lanGroup = &net.UDPAddr{IP: net.IPv4(239, 255, 70, 65), Port: 47655}

type lanSignal struct {
	*inbox
	conn *net.UDPConn
}

// NewLAN joins the multicast group. An error means there is no LAN signaling.
func NewLAN() (Signal, error) {
	conn, err := net.ListenMulticastUDP("udp4", nil, lanGroup)
	if err != nil {
		return nil, err
	}
	// Go's ListenMulticastUDP disables loopback; re-enable so instances on one host see each other.
	if err := ipv4.NewPacketConn(conn).SetMulticastLoopback(true); err != nil {
		conn.Close()
		return nil, err
	}
	l := &lanSignal{inbox: newInbox(), conn: conn}
	go l.read()
	return l, nil
}

func (l *lanSignal) read() {
	buf := make([]byte, 64*1024)
	for {
		n, _, err := l.conn.ReadFromUDP(buf)
		if err != nil {
			return // closed
		}
		var ev nostr.Event
		if json.Unmarshal(buf[:n], &ev) != nil {
			continue
		}
		if ok, err := ev.CheckSignature(); err != nil || !ok {
			continue
		}
		l.deliver(ev)
	}
}

func (l *lanSignal) Publish(_ context.Context, ev nostr.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.markSeen(ev.ID) // ignore our own loopback
	l.mu.Unlock()
	_, err = l.conn.WriteToUDP(b, lanGroup)
	return err
}

func (l *lanSignal) Subscribe(pubs []string)    { l.setSubs(pubs) }
func (l *lanSignal) Events() <-chan nostr.Event { return l.out }
func (l *lanSignal) Close() error {
	err := l.conn.Close()
	l.close()
	return err
}
