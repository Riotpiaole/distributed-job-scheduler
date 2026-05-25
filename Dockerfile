## Stage 1: Build the binary and plugins
FROM golang:1.26-alpine AS builder

# build-base provides gcc + musl-dev, required for CGO (Go plugin support).
RUN apk add --no-cache build-base

WORKDIR /src

COPY . .

RUN go mod tidy


# Build the main binary.
RUN CGO_ENABLED=1 GOOS=linux go build -o /go-flink .

# Build the bundled wc plugin. Additional plugins can be dropped in /plugins at runtime.
RUN CGO_ENABLED=1 GOOS=linux go build -buildmode=plugin -o /plugins/wc.so ./plugin/

## Stage 2: Minimal runtime image
FROM alpine:3.19

# libstdc++ and libgcc are needed at runtime for .so plugins loaded via CGO.
RUN apk add --no-cache ca-certificates libstdc++ libgcc

COPY --from=builder /go-flink /usr/local/bin/go-flink
COPY --from=builder /plugins   /plugins

# Default mount points (overridden by PVC mounts in k8s).
RUN mkdir -p /data/raft /data/input /data/output

ENTRYPOINT ["/usr/local/bin/go-flink"]
CMD ["--help"]
