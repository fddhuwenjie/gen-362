FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /temporal-lite-server ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -o /temporal-lite-cli ./cmd/cli

FROM alpine:3.20

WORKDIR /app

RUN apk --no-cache add ca-certificates postgresql-client

COPY --from=builder /temporal-lite-server /usr/local/bin/temporal-lite-server
COPY --from=builder /temporal-lite-cli /usr/local/bin/temporal-lite
COPY ./migrations /app/migrations

ENV DB_CONN="postgres://postgres:postgres@postgres:5432/temporal_lite?sslmode=disable"
ENV PORT="8132"
ENV MIGRATIONS_DIR="/app/migrations"

EXPOSE 8132

CMD ["temporal-lite-server"]
