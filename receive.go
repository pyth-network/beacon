package main

import (
	"context"
	"fmt"

	gossipv1 "github.com/certusone/wormhole/node/pkg/proto/gossip/v1"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	libp2ptls "github.com/libp2p/go-libp2p/p2p/security/tls"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	libp2pquicreuse "github.com/libp2p/go-libp2p/p2p/transport/quicreuse"
	"github.com/multiformats/go-multiaddr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
	"google.golang.org/protobuf/proto"
)

func ReceiveMessages(channel chan *vaa.VAA, heartbeat *Heartbeat, networkID string, bootstrapAddrs, listenAddrs []string) {
	ctx := context.Background()

	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to generate key pair")
	}

	mgr, err := connmgr.NewConnManager(
		100,
		400,
		connmgr.WithGracePeriod(0),
	)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create libp2p connection manager")
	}

	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listenAddrs...),
		libp2p.Security(libp2ptls.ID, libp2ptls.New),
		// Disable Reuse because upon panic, the Close() call on the p2p reactor does not properly clean up the
		// open ports (they are kept around for re-use, this seems to be a libp2p bug in the reuse `gc()` call
		// which can be found here:
		//
		// https://github.com/libp2p/go-libp2p/blob/master/p2p/transport/quicreuse/reuse.go#L97
		//
		// By disabling this we get correct Close() behaviour.
		//
		// IMPORTANT: Normally re-use allows libp2p to dial on the same port that is used to listen for traffic
		// and by disabling this dialing uses a random high port (32768-60999) which causes the nodes that we
		// connect to by dialing (instead of them connecting to us) will respond on the high range port instead
		// of the specified Dial port. This requires firewalls to be configured to allow (UDP 32768-60999) which
		// should be specified in our documentation.
		//
		// The best way to securely enable this range is via the conntrack module, which can statefully allow
		// UDP packets only when a sent UDP packet is present in the conntrack table. This rule looks roughly
		// like this:
		//
		// iptables -A INPUT -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT
		//
		// Which is a standard rule in many firewall configurations (RELATED is the key flag).
		libp2p.QUICReuse(libp2pquicreuse.NewConnManager, libp2pquicreuse.DisableReuseport()),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.ConnectionManager(mgr),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			bootstrappers := make([]peer.AddrInfo, 0)

			for _, addr := range bootstrapAddrs {
				ma, err := multiaddr.NewMultiaddr(addr)

				if err != nil {
					continue
				}

				pi, err := peer.AddrInfoFromP2pAddr(ma)

				if err != nil || pi.ID == h.ID() {
					continue
				}

				bootstrappers = append(bootstrappers, *pi)
			}

			idht, err := dht.New(ctx, h, dht.Mode(dht.ModeServer),
				dht.ProtocolPrefix(protocol.ID("/"+networkID)),
				dht.BootstrapPeers(bootstrappers...),
			)

			return idht, err
		}),
	)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create libp2p host")
	}

	defer h.Close()

	topic := fmt.Sprintf("%s/%s", networkID, "broadcast")
	ps, err := pubsub.NewGossipSub(ctx, h, pubsub.WithValidateQueueSize(1024))

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create gossip pubsub")
	}

	th, err := ps.Join(topic)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to join pubsub topic")
	}

	defer th.Close()

	sub, err := th.Subscribe(pubsub.WithBufferSize(1024))

	if err != nil {
		log.Panic().Err(err).Msg("Failed to subscribe to pubsub topic")
	}

	defer sub.Cancel()

	messagesMetric := promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_messages",
		Help: "Count of messages received from the p2p network",
	})

	for {
		select {
		case <-ctx.Done():
			return
		default:
			envelope, err := sub.Next(ctx)

			if err != nil {
				log.Panic().Err(err).Msg("Failed to receive pubsub message")
			}

			var msg gossipv1.GossipMessage
			err = proto.Unmarshal(envelope.Data, &msg)

			if err != nil {
				log.Panic().Err(err).Msg("Failed to unmarshal pubsub message")
			}

			switch msg.Message.(type) {
			case *gossipv1.GossipMessage_SignedObservation:
			case *gossipv1.GossipMessage_SignedVaaWithQuorum:
				vaaBytes := msg.GetSignedVaaWithQuorum().GetVaa()
				vaa, err := vaa.Unmarshal(vaaBytes)

				if err != nil {
					log.Error().Err(err).Msg("Failed to parse VAA")
					continue
				}

				// Send message on channel, increment counter, and update heartbeat
				channel <- vaa
				messagesMetric.Inc()
				if vaa.Timestamp.Unix() > heartbeat.Timestamp {
					heartbeat.Timestamp = vaa.Timestamp.Unix()
				}

				log.Debug().Str("id", vaa.MessageID()).Msg("Received message")
			}
		}
	}
}
