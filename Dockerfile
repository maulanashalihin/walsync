FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o walsync .

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /build/walsync /usr/local/bin/walsync
ENTRYPOINT ["walsync"]
