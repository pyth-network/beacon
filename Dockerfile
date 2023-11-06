FROM docker.io/golang:1.20.10-bullseye@sha256:082569b3303b164cc4a7c88ac59b19b69c1a5d662041ac0dca046ac239632442

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /beacon

# Run
CMD ["/beacon"]
