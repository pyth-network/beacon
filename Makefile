beacon: main.go receive.go write.go serve.go
	go build

$PHONY: run run-nats

run: beacon
	./beacon

run-nats:
	nats-server --jetstream
