package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"
	"github.com/rs/zerolog"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
)

var cli struct {
	WormholeNetworkID string   `kong:"required,env='WORMHOLE_NETWORK_ID',help='Wormhole network ID'"`
	WormholeBootstrap []string `kong:"required,env='WORMHOLE_BOOTSTRAP',help='Bootstrap nodes to connect to.'"`
	WormholeListen    []string `kong:"required,env='WORMHOLE_LISTEN',help='Addresses to listen on'"`
	ServerURL         string   `kong:"required,env='SERVER_URL',help='gRPC server URL to bind'"`
	NatsURL           string   `kong:"required,env='NATS_URL',help='NATS URL to connect'"`
	WriterBatchSize   int      `kong:"required,env='WRITER_BATCH_SIZE',default=100,help='Number of messages to batch'"`
	LogLevel          string   `kong:"required,env='LOG_LEVEL',default=info,help='Log level'"`
}

const STREAM_NAME = "VAAS"

func main() {
	kong.Parse(&cli)

	logLevels := map[string]zerolog.Level{
		"warn":  zerolog.WarnLevel,
		"info":  zerolog.InfoLevel,
		"debug": zerolog.DebugLevel,
	}

	zerolog.SetGlobalLevel(logLevels[strings.ToLower(cli.LogLevel)])
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	channel := make(chan *vaa.VAA)

	go ReceiveMessages(channel, cli.WormholeNetworkID, cli.WormholeBootstrap, cli.WormholeListen)
	go WriteMessages(channel, cli.NatsURL, cli.WriterBatchSize)
	go ServeMessages(cli.ServerURL, cli.NatsURL)

	// Interrupt on CTRL-C
	done := make(chan os.Signal, 1)

	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-done
}
