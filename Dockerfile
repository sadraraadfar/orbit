# syntax=docker/dockerfile:1

# Build stage. SERVICE selects which command to compile.
FROM golang:1.27-alpine AS build
WORKDIR /src

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/orbit ./cmd/${SERVICE}

# Runtime stage: small Alpine image with a non-root user and a healthcheck.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && \
    addgroup -S orbit && adduser -S -G orbit orbit
COPY --from=build /out/orbit /usr/local/bin/orbit
USER orbit
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=5 \
  CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1
ENTRYPOINT ["/usr/local/bin/orbit"]
