# Build from the repository root:
# docker build -f examples/kubernetes-sidecar/Dockerfile -t chronicle-kubernetes-sidecar:local .
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY . .
WORKDIR /src/examples/kubernetes-sidecar
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/chronicle-sidecar-example .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/chronicle-sidecar-example /chronicle-sidecar-example
EXPOSE 9090 8080 8086
ENTRYPOINT ["/chronicle-sidecar-example"]
CMD ["-mode=sidecar", "-data-dir=/data"]
