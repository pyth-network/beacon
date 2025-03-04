package main

import (
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/wormhole-foundation/wormhole/sdk/vaa"
)

var cli struct {
	NodeKeyPath        string `kong:"optional,env='NODE_KEY_PATH',help='Path to the node key. If set and key doesn't exists there it gets created. If not set, a new key will be generated'"`
	WormholeEnv        string `kong:"optional,env='WORMHOLE_ENV',help='Wormhole environment (may be \"testnet\" or \"mainnet\") required if WORMHOLE_NETWORK_ID and WORMHOLE_BOOTSTRAP is not set'"`
	WormholeNetworkID  string `kong:"optional,env='WORMHOLE_NETWORK_ID',help='Wormhole network ID, required if WORMHOLE_ENV is not set'"`
	WormholeBootstrap  string `kong:"optional,env='WORMHOLE_BOOTSTRAP',help='Bootstrap nodes to connect to. Required if WORMHOLE_ENV is not set'"`
	WormholeListenPort uint   `kong:"required,env='WORMHOLE_LISTEN_PORT',default='8999',help='Port to listen on'"`
	ServerURL          string `kong:"required,env='SERVER_URL',help='gRPC server URL to bind'"`
	NatsStream         string `kong:"required,env='NATS_STREAM',help='NATS stream to use'"`
	NatsURL            string `kong:"required,env='NATS_URL',help='NATS URL to connect'"`
	WriterBatchSize    int    `kong:"required,env='WRITER_BATCH_SIZE',default=100,help='Number of messages to batch'"`
	LogLevel           string `kong:"required,env='LOG_LEVEL',default=info,help='Log level'"`
	MetricsURL         string `kong:"required,env='METRICS_URL',default=':8081',help='Metrics URL to bind'"`
	HeartbeatURL       string `kong:"required,env='HEARTBEAT_URL',default=':9000',help='Heartbeat URL to bind'"`
	HeartbeatInterval  int    `kong:"required,env='HEARTBEAT_INTERVAL',default='10',help='Maximum time between heartbeats in seconds'"`
}

type Heartbeat struct {
	// Timestamp is updated from the ReceiveMessages thread
	Timestamp int64
	Interval  int
}

func (h *Heartbeat) Handle(w http.ResponseWriter, r *http.Request) {
	lastHeartbeat := time.Unix(h.Timestamp, 0)
	interval := time.Since(lastHeartbeat)

	if interval < time.Duration(h.Interval)*time.Second {
		log.Debug().Dur("interval_ms", interval).Msg("Heartbeat succeeded")
		w.WriteHeader(200)
	} else {
		log.Error().Dur("interval_ms", interval).Msg("Heartbeat failed")
		w.WriteHeader(503)
	}
}

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
	heartbeat := &Heartbeat{0, cli.HeartbeatInterval}

	go func() {
		log.Info().Str("url", cli.MetricsURL).Msg("Starting metrics server")
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(cli.MetricsURL, nil)
		if err != nil {
			log.Panic().Err(err).Msg("Failed to start metrics server")
		}
	}()

	go func() {
		log.Info().Str("url", cli.HeartbeatURL).Msg("Starting heartbeat server")
		http.HandleFunc("/", heartbeat.Handle)
		err := http.ListenAndServe(cli.HeartbeatURL, nil)
		if err != nil {
			log.Panic().Err(err).Msg("Failed to start heartbeat server")
		}
	}()

	log.Info().Msg("Starting receive/write/serve goroutines")
	go ReceiveMessages(channel, heartbeat, cli.NodeKeyPath, cli.WormholeEnv, cli.WormholeNetworkID, cli.WormholeBootstrap, cli.WormholeListenPort)
	go WriteMessages(channel, cli.NatsURL, cli.NatsStream, cli.WriterBatchSize)
	go ServeMessages(cli.ServerURL, cli.NatsURL, cli.NatsStream)

	// Interrupt on CTRL-C
	done := make(chan os.Signal, 1)

	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-done
}
