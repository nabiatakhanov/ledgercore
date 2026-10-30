FROM golang:1.26.2-bookworm@sha256:47ce5636e9936b2c5cbf708925578ef386b4f8872aec74a67bd13a627d242b19 AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Integration tests use the Go image's C compiler for the race detector.
FROM source AS integration
CMD ["go", "test", "-p", "1", "-race", "-count=1", "./..."]

FROM source AS build
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM debian:12.13-slim@sha256:67b30a61dc87758f0caf819646104f29ecbda97d920aaf5edc834128ac8493d3 AS runtime
WORKDIR /app
COPY --from=build --chmod=0555 /out/ /app/
COPY --chmod=0555 scripts/healthcheck.sh /app/healthcheck.sh
USER 65532:65532
ENV GOMEMLIMIT=192MiB
EXPOSE 8080 9090 9091 9092 9093 50051 50052
CMD ["/app/gateway"]
