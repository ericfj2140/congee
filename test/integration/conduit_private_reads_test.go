package integration_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/gorilla/websocket"
	"github.com/michmich112/congee/internal/config"
	"github.com/michmich112/congee/internal/db"
	"github.com/michmich112/congee/internal/nips"
	"github.com/michmich112/congee/internal/relay"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rs/zerolog"
)

var _ = Describe("Conduit public access and private message reads", func() {
	DescribeTable("keeps guest requests open and serves gift wraps only to authenticated recipients", func(challengeOnConnect bool) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		Expect(err).NotTo(HaveOccurred())
		url := fmt.Sprintf("ws://%s/", ln.Addr())
		cfg := config.DefaultConfig()
		cfg.NIPs.Enabled = []int{1, 11, 17, 42, 50}
		cfg.NIP42.RelayURL = url
		cfg.NIP42.SendChallengeOnConnect = challengeOnConnect
		Expect(cfg.Validate()).To(Succeed())
		st, closeStore, err := db.OpenTestStore(context.Background(), filepath.Join(GinkgoT().TempDir(), "events.db"), zerolog.Nop())
		Expect(err).NotTo(HaveOccurred())
		defer closeStore()
		srv, err := relay.NewServer(cfg, st, zerolog.Nop(), nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(nips.LoadEnabled(cfg, srv, st, zerolog.Nop())).To(Succeed())
		go func() { _ = srv.Serve(ln) }()
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			Expect(srv.Shutdown(ctx)).To(Succeed())
		}()
		read := func(c *websocket.Conn) []any {
			Expect(c.SetReadDeadline(time.Now().Add(3 * time.Second))).To(Succeed())
			_, data, err := c.ReadMessage()
			Expect(err).NotTo(HaveOccurred())
			var msg []any
			Expect(json.Unmarshal(data, &msg)).To(Succeed())
			return msg
		}
		send := func(c *websocket.Conn, frame []any) { Expect(c.WriteJSON(frame)).To(Succeed()) }
		dial := func() (*websocket.Conn, string) {
			c, _, err := websocket.DefaultDialer.Dial(url, nil)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(c.Close)
			challenge := ""
			if challengeOnConnect {
				msg := read(c)
				Expect(msg[0]).To(Equal("AUTH"))
				challenge = msg[1].(string)
			}
			return c, challenge
		}
		requireAuth := func(c *websocket.Conn, challenge string) string {
			send(c, []any{"REQ", "inbox", map[string]any{"kinds": []int{1059}}})
			if challenge == "" {
				msg := read(c)
				Expect(msg[0]).To(Equal("AUTH"))
				challenge = msg[1].(string)
			}
			msg := read(c)
			Expect(msg[0]).To(Equal("CLOSED"))
			Expect(msg[2]).To(HavePrefix("auth-required:"))
			return challenge
		}
		authenticate := func(c *websocket.Conn, key *btcec.PrivateKey, challenge string) {
			auth := nip42AuthEvent(key, url, challenge, time.Now().Unix())
			send(c, []any{"AUTH", auth})
			msg := read(c)
			Expect(msg[0]).To(Equal("OK"))
			Expect(msg[1]).To(Equal(auth.ID))
			Expect(msg[2]).To(BeTrue())
		}
		guest, challenge := dial()
		merchant, err := btcec.NewPrivateKey()
		Expect(err).NotTo(HaveOccurred())
		wrapper, err := btcec.NewPrivateKey()
		Expect(err).NotTo(HaveOccurred())
		recipient := hex.EncodeToString(merchant.PubKey().SerializeCompressed()[1:])
		wrap := signedEvent(wrapper, 1059, "synthetic encrypted order envelope", [][]string{{"p", recipient}})
		send(guest, []any{"EVENT", wrap})
		ack := read(guest)
		Expect(ack[0]).To(Equal("OK"))
		Expect(ack[1]).To(Equal(wrap.ID))
		Expect(ack[2]).To(BeTrue()) // Guest delivery never needs AUTH.

		product := signedEvent(wrapper, 30402, "coffee beans", [][]string{{"d", "coffee"}})
		Expect(st.SaveEvent(context.Background(), &product)).To(Succeed())
		publicREQ := []any{"REQ", "catalog", map[string]any{"kinds": []int{30402}, "search": "coffee", "limit": 30}}
		send(guest, publicREQ)
		msg := read(guest)
		Expect(msg[0]).To(Equal("EVENT"))
		Expect(msg[2].(map[string]any)["id"]).To(Equal(product.ID))
		Expect(read(guest)[0]).To(Equal("EOSE"))
		challenge = requireAuth(guest, challenge)
		// Rejecting an inbox request must not poison the public connection.
		send(guest, publicREQ)
		Expect(read(guest)[0]).To(Equal("EVENT"))
		Expect(read(guest)[0]).To(Equal("EOSE"))

		// An authenticated non-recipient still cannot read the private envelope.
		authenticate(guest, wrapper, challenge)
		inboxREQ := []any{"REQ", "inbox", map[string]any{"kinds": []int{1059}, "#p": []string{recipient}}}
		send(guest, inboxREQ)
		Expect(read(guest)[0]).To(Equal("EOSE"))

		inbox, inboxChallenge := dial()
		inboxChallenge = requireAuth(inbox, inboxChallenge)
		authenticate(inbox, merchant, inboxChallenge)
		send(inbox, inboxREQ)
		msg = read(inbox)
		Expect(msg[0]).To(Equal("EVENT"))
		Expect(msg[2].(map[string]any)["id"]).To(Equal(wrap.ID))
		Expect(read(inbox)[0]).To(Equal("EOSE"))

		// Live delivery uses the same recipient check as stored history.
		next := signedEvent(wrapper, 1059, "second synthetic encrypted envelope", [][]string{{"p", recipient}})
		send(guest, []any{"EVENT", next})
		Expect(read(guest)[0]).To(Equal("OK"))
		msg = read(inbox)
		Expect(msg[0]).To(Equal("EVENT"))
		Expect(msg[2].(map[string]any)["id"]).To(Equal(next.ID))
	}, Entry("lazy challenge", false), Entry("optional connect-time challenge", true))
})
