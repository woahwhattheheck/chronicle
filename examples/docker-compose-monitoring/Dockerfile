FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /chronicle-compose ./examples/docker-compose-monitoring/chronicle

FROM gcr.io/distroless/static-debian12
COPY --from=builder /chronicle-compose /chronicle-compose
EXPOSE 8086
ENTRYPOINT ["/chronicle-compose"]
