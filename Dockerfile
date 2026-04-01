# Build stage
FROM golang:1.25-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# Copy go mod files first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build all binaries with optimizations
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -o fleuve-runner ./cmd/runner

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -o fleuve-gateway ./cmd/gateway

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -o fleuve-ui ./examples/ui_server

# Runtime stage
FROM alpine:3.19

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /build/fleuve-runner /app/
COPY --from=builder /build/fleuve-gateway /app/
COPY --from=builder /build/fleuve-ui /app/

# Create non-root user for security
RUN addgroup -g 1000 fleuve && \
    adduser -D -u 1000 -G fleuve fleuve && \
    chown -R fleuve:fleuve /app

USER fleuve

# Default command (can be overridden in docker-compose)
CMD ["./fleuve-runner"]
