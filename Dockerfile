FROM golang:1.26.2-bookworm@sha256:47ce5636e9936b2c5cbf708925578ef386b4f8872aec74a67bd13a627d242b19 AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Integration tests use the Go image's C compiler for the race detector.
FROM source AS integration
CMD ["go", "test", "-race", "-count=1", "./tests"]

FROM source AS build
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ledgerctl ./cmd/ledgerctl

FROM debian:12.13-slim@sha256:67b30a61dc87758f0caf819646104f29ecbda97d920aaf5edc834128ac8493d3 AS runtime
WORKDIR /app
COPY --from=build --chmod=0555 /out/api /out/ledgerctl /app/
COPY --chmod=0555 scripts/healthcheck.sh /app/healthcheck.sh
USER 65532:65532
EXPOSE 8080 9090
CMD ["/app/api"]
