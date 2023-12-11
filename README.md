# Pyth Beacon

**Beacon** is a highly available version of [Wormhole Spy](https://docs.wormhole.com/wormhole/explore-wormhole/spy). It
connects to the Wormhole peer-to-peer gossip network, providing the Spy GRPC API to clients for fetching Wormhole VAAs.
Beacon uses [NATS](https://nats.io/) as a message broker to aggregate VAAs from multiple clients, minimizing latency and
maximizing availability.

# Why Beacon?

Beacon enhances the Wormhole Spy for high availability and reduced latency. By deploying multiple replicas, it mitigates
the unpredictable latency of a single Spy instance due to the peer-to-peer network's peer selection algorithm. Beacon
aggregates VAAs in a NATS stream while deduplicating them, ensuring minimal latency and maximum availability. It
includes a health check mechanism to detect and address high latency issues.

# How to use Beacon?

To run Beacon, install `nats-server` and then run it using this command:

```shell
nats-server --jetstream
```

Configure Beacon by setting the required environment variables in [.envrc.sample](./.envrc.sample), including `NATS_URL`
and `NATS_STREAM`. Then, start Beacon with the following command:

```shell
make run
```

Alternatively, you can use the Docker image of Beacon published [here](https://gallery.ecr.aws/pyth-network/beacon).

# Using Beacon in Production

For optimal performance, co-locate Beacon with the NATS server to minimize overal latency. Run multiple Beacon
instances to increase availability and reduce latency, but be mindful that this will scale network traffic. Utilize the
health check probe to monitor VAA latency and restart instances if they become unhealthy.

# It works with Go v1.20.12 and older