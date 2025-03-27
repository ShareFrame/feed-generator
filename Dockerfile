# syntax=docker/dockerfile:1
FROM golang:1.23.5 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the Go binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o firehose ./cmd/main.go

# Minimal image
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/firehose .

CMD ["./firehose"]
