FROM golang:1.27.3-alpine AS builder

WORKDIR /src
RUN apk add --no-cache ca-certificates

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/spot-scanner ./cmd/spot-scanner

FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata
RUN addgroup -S app && adduser -S -G app app
RUN mkdir -p /data && chown app:app /data

WORKDIR /app
COPY --from=builder /out/spot-scanner /app/spot-scanner
COPY migrations /app/migrations

USER app
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 CMD wget -q -O /dev/null http://127.0.0.1:8080/health/ready || exit 1

ENTRYPOINT ["/app/spot-scanner"]
