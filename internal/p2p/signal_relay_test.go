package p2p

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// TestRelayResubscribeNoDoubleClose: go-nostr's SubscribeMany over several relays can
// close its event channel twice when the workers exit together; one call per relay cannot.
func TestRelayResubscribeNoDoubleClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		for { // a silent relay: read and drop everything
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	base := "ws" + strings.TrimPrefix(srv.URL, "http")
	var urls []string
	for i := 0; i < 16; i++ {
		urls = append(urls, fmt.Sprintf("%s/%d", base, i))
	}
	r := NewRelays(urls)
	defer r.Close()
	for i := 0; i < 100; i++ {
		r.Subscribe([]string{fmt.Sprintf("%064x", i+1)})
		time.Sleep(20 * time.Millisecond)
	}
}
