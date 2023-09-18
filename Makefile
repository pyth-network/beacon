beacon: main.go receiver.go writer.go server.go
	go build

$PHONY: run
run: beacon
	./beacon --bootstrap=/dns4/wormhole-mainnet-v2-bootstrap.certus.one/udp/8999/quic/p2p/12D3KooWQp644DK27fd3d4Km3jr7gHiuJJ5ZGmy8hH4py7fP4FP7 --network-id=/wormhole/mainnet/2 --listen=/ip4/0.0.0.0/udp/30910/quic