# Binary-wrap: cross-compile sandbox-loadgen on the build host, then COPY into
# distroless. Build context must contain the prebuilt linux/amd64 binary.
#   GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" \
#     -o sandbox-loadgen ./cmd/sandbox-loadgen/
#   docker build -f deploy/loadgen/Dockerfile -t <cr>/g0031/sandbox-loadgen:<tag> .
FROM gcr.io/distroless/static:nonroot
COPY sandbox-loadgen /usr/local/bin/sandbox-loadgen
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/sandbox-loadgen"]
