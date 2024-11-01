FROM docker.io/golang:1.21.9-bullseye@sha256:e249f1c6bcb9bf18a7fab1b78456f91f5e0e6be80f71485efb8b1e1a5c72f3db

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY *.go ./

# Build
RUN CGO_ENABLED=1 GOOS=linux go build -o /beacon

# Run
CMD ["/beacon"]
