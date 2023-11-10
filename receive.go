package main

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/certusone/wormhole/node/pkg/common"
	"github.com/certusone/wormhole/node/pkg/p2p"
	gossipv1 "github.com/certusone/wormhole/node/pkg/proto/gossip/v1"
	"github.com/certusone/wormhole/node/pkg/supervisor"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
	"go.uber.org/zap"
)

func ReceiveMessages(channel chan *vaa.VAA, heartbeat *Heartbeat, networkID, bootstrapAddrs string, listenPort uint) {
	common.SetRestrictiveUmask()

	// Node's main lifecycle context.
	rootCtx, rootCtxCancel := context.WithCancel(context.Background())
	defer rootCtxCancel()

	// Outbound gossip message queue
	sendC := make(chan []byte)

	// Inbound observations
	obsvC := make(chan *common.MsgWithTimeStamp[gossipv1.SignedObservation], 1024)

	// Inbound observation requests
	obsvReqC := make(chan *gossipv1.ObservationRequest, 1024)

	// Inbound signed VAAs
	signedInC := make(chan *gossipv1.SignedVAAWithQuorum, 1024)

	// Guardian set state managed by processor. We do not have a way to fetch
	// it; so we will use a nil guardian set state
	gst := common.NewGuardianSetState(nil)

	messagesMetric := promauto.NewCounter(prometheus.CounterOpts{
		Name: "beacon_messages",
		Help: "Count of messages received from the p2p network",
	})

	messageLatencyMetric := promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "beacon_message_latency",
		Help:    "Latency of messages received from the p2p network",
		Buckets: []float64{0.3, 0.7, 1, 1.3, 1.7, 2, 2.3, 2.7, 3, 3.5, 4, 5, 10, 20},
	})

	observationsMetric := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_observations",
		Help: "Count of observations received from the p2p network",
	}, []string{"guardian"})

	// Update metrics for observation to track guardians behaviour
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case o := <-obsvC:
				guardian := hex.EncodeToString(o.Msg.Addr)
				observationsMetric.WithLabelValues(guardian).Inc()
			}
		}
	}()

	// Ignore observation requests
	// Note: without this, the whole program hangs on observation requests
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case <-obsvReqC:
			}
		}
	}()

	// Log signed VAAs
	go func() {
		for {
			select {
			case <-rootCtx.Done():
				return
			case v := <-signedInC:
				vaaBytes := v.GetVaa()
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

					// Only count the latency for the newer messages. This will
					// give a better indication of the lag in the network.
					//
					// Also, we might receive a single message multiple times and doing the check
					// here is that we only count the latency once.
					messageLatencyMetric.Observe(time.Since(time.Unix(vaa.Timestamp.Unix(), 0)).Seconds())
				}

				log.Debug().Str("id", vaa.MessageID()).Msg("Received message")
			}
		}
	}()

	// Create a random private key
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		log.Panic().Err(err).Msg("Failed to create private key")
	}

	// TODO: use zap logger everywhere
	logger, err := zap.NewProductionConfig().Build()
	if err != nil {
		log.Panic().Err(err).Msg("Failed to create logger")
	}

	// Run supervisor.
	supervisor.New(rootCtx, logger, func(ctx context.Context) error {
		components := p2p.DefaultComponents()
		components.Port = listenPort
		if err := supervisor.Run(ctx,
			"p2p",
			p2p.Run(obsvC,
				obsvReqC,
				nil,
				sendC,
				signedInC,
				priv,
				nil,
				gst,
				networkID,
				bootstrapAddrs,
				"",
				false,
				rootCtxCancel,
				nil,
				nil,
				nil,
				nil,
				components,
				nil,   // ibc feature string
				false, // gateway relayer enabled
				false, // ccqEnabled
				nil,   // query requests
				nil,   // query responses
				"",    // query bootstrap peers
				0,     // query port
				"",    // query allow list
			)); err != nil {
			return err
		}

		log.Info().Msg("Started internal services")

		<-ctx.Done()
		return nil
	},
		// It's safer to crash and restart the process in case we encounter a panic,
		// rather than attempting to reschedule the runnable.
		supervisor.WithPropagatePanic)

	<-rootCtx.Done()
	log.Info().Msg("root context cancelled, exiting...")
}
