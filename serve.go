package main

import (
	"context"
	"fmt"
	"net"
	"time"

	spyv1 "github.com/certusone/wormhole/node/pkg/proto/spy/v1"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type filterSignedVaa struct {
	chainId     vaa.ChainID
	emitterAddr vaa.Address
}

type server struct {
	spyv1.UnimplementedSpyRPCServiceServer
	natsConn *nats.Conn
}

func (s server) SubscribeSignedVAA(req *spyv1.SubscribeSignedVAARequest, server spyv1.SpyRPCService_SubscribeSignedVAAServer) error {
	var fi []filterSignedVaa
	if req.Filters != nil {
		for _, f := range req.Filters {
			switch t := f.Filter.(type) {
			case *spyv1.FilterEntry_EmitterFilter:
				addr, err := vaa.StringToAddress(t.EmitterFilter.EmitterAddress)
				if err != nil {
					return status.Error(codes.InvalidArgument, fmt.Sprintf("failed to decode emitter address: %v", err))
				}
				fi = append(fi, filterSignedVaa{
					chainId:     vaa.ChainID(t.EmitterFilter.ChainId),
					emitterAddr: addr,
				})
			default:
				return status.Error(codes.InvalidArgument, "unsupported filter type")
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()

	js, err := jetstream.New(s.natsConn)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create stream context")
	}

	stream, err := js.Stream(ctx, STREAM_NAME)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create stream object")
	}

	consumer, err := stream.CreateOrUpdateConsumer(
		ctx,
		jetstream.ConsumerConfig{
			AckPolicy:         jetstream.AckNonePolicy,
			DeliverPolicy:     jetstream.DeliverNewPolicy,
			InactiveThreshold: 5 * time.Second,
		},
	)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to create/update stream consumer")
	}

	for {
		message, err := consumer.Next()

		if err != nil {
			log.Error().Err(err).Msg("Failed to consume stream message")
			continue
		}

		vaaBytes := message.Data()
		vaaMsg, err := vaa.Unmarshal(vaaBytes)

		if err != nil {
			log.Error().Err(err).Msg("Failed to unmarshal VAA")
			continue
		}

		// Check if the VAA matches any of the filters
		var found bool = false
		for _, f := range fi {
			if f.chainId == vaaMsg.EmitterChain && f.emitterAddr == vaaMsg.EmitterAddress {
				found = true
				break
			}
		}

		// If we didn't find a match, and there are filters, skip this message.
		if !found && len(fi) > 0 {
			continue
		}

		err = server.Send(&spyv1.SubscribeSignedVAAResponse{VaaBytes: vaaBytes})

		if err != nil {
			if status.Code(err) == codes.Canceled || status.Code(err) == codes.Unavailable {
				log.Debug().Err(err).Str("id", vaaMsg.MessageID()).Msg("Client cancelled or unexpectedly closed subscription")
				return nil
			}

			log.Error().Err(err).Str("id", vaaMsg.MessageID()).Msg("Failed to send message to client")
			continue
		}

		log.Debug().Str("id", vaaMsg.MessageID()).Msg("Sent message")
	}
}

func ServeMessages(serverURL, natsURL string) {
	nc, err := nats.Connect(natsURL)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to connect to NATS server")
	}

	listener, err := net.Listen("tcp", serverURL)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to bind gRPC socket")
	}

	grpcServer := grpc.NewServer()
	server := server{natsConn: nc}

	reflection.Register(grpcServer)
	spyv1.RegisterSpyRPCServiceServer(grpcServer, server)

	err = grpcServer.Serve(listener)

	if err != nil {
		log.Panic().Err(err).Msg("Failed to start gRPC server")
	}
}
