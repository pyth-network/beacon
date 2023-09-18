package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
)

func WriteMessages(channel chan *vaa.VAA, natsURL string, batchSize int) {
	nc, err := nats.Connect(natsURL)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to NATS server")
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(4096))

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create stream context")
	}

	stream, _ := js.StreamInfo(STREAM_NAME)

	if stream == nil {
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     STREAM_NAME,
			MaxBytes: 4_000_000_000,
		})

		if err != nil {
			log.Panic().Err(err).Str("stream", STREAM_NAME).Msg("Failed to create stream")
		}
	}

	counter := 0

	for message := range channel {
		bytes, err := message.Marshal()

		if err != nil {
			log.Panic().Err(err).Str("id", message.MessageID()).Msg("Failed to marshal VAA")
		}

		_, err = js.PublishMsgAsync(&nats.Msg{
			Subject: STREAM_NAME,
			Header:  nats.Header{"Nats-Msg-Id": []string{message.MessageID()}},
			Data:    bytes,
		})

		if err != nil {
			log.Error().Err(err).Str("id", message.MessageID()).Msg("Failed to publish message")
		} else {
			log.Debug().Str("id", message.MessageID()).Msg("Published message")
		}

		counter++

		if counter%batchSize == 0 {
			startTime := time.Now()

			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(500 * time.Millisecond):
				log.Warn().Msg("Ack await took longer than 500 milliseconds")
			}

			ackTime := float64(time.Since(startTime).Microseconds()) / float64(1000)

			log.Info().Float64("ack_time_ms", ackTime).Msg(fmt.Sprintf("Acked %d messages", batchSize))
		}
	}
}
