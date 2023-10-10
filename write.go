package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog/log"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
)

func WriteMessages(channel chan *vaa.VAA, natsURL string, natsStream string, batchSize int) {
	nc, err := nats.Connect(natsURL)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to NATS server")
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create stream context")
	}

	stream, _ := js.StreamInfo(natsStream)

	if stream == nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     natsStream,
			MaxBytes: 4_000_000_000,
		})

		if err != nil {
			log.Panic().Err(err).Str("stream", natsStream).Msg("Failed to create stream")
		}
	}

	batchCounter := 0

	streamAckLatencyMetric := promauto.NewSummary(prometheus.SummaryOpts{
		Name: "beacon_stream_ack_latency",
		Help: "Latency (milliseconds) of batch acks from the stream server",
	})

	for message := range channel {
		bytes, err := message.Marshal()

		if err != nil {
			log.Panic().Err(err).Str("id", message.MessageID()).Msg("Failed to marshal VAA")
		}

		// Use the signing digest of the VAA as the NATS message ID to prevent
		// DoS against valid messages by taking advantage of stream
		// deduplication.
		signingDigest := message.SigningDigest().Hex()

		_, err = js.PublishMsgAsync(&nats.Msg{
			Subject: natsStream,
			Header:  nats.Header{"Nats-Msg-Id": []string{signingDigest}},
			Data:    bytes,
		})

		if err != nil {
			log.Error().Str("id", message.MessageID()).Str("signing_digest", signingDigest).Err(err).Msg("Failed to publish message")
		} else {
			log.Debug().Str("id", message.MessageID()).Str("signing_digest", signingDigest).Msg("Published message")
		}

		batchCounter++

		if batchCounter%batchSize == 0 {
			startTime := time.Now()

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(500 * time.Millisecond):
				log.Warn().Msg("Ack await took longer than 500 milliseconds")
			}

			ackTime := float64(time.Since(startTime).Microseconds()) / float64(1000)

			streamAckLatencyMetric.Observe(ackTime)
			log.Debug().Float64("ack_time_ms", ackTime).Msg(fmt.Sprintf("Acked %d messages", batchSize))
		}
	}
}
