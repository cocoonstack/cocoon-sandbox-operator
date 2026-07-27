# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.26.5 AS builder

ARG TARGETOS=linux
ARG TARGETARCH
ARG GIT_VERSION=unknown
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown

WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download
COPY api/ ./api/
COPY cmd/ ./cmd/
COPY controllers/ ./controllers/
COPY extensions/ ./extensions/
COPY internal/ ./internal/
COPY pkg/ ./pkg/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w -X github.com/cocoonstack/sandbox-operator/internal/version.gitVersion=${GIT_VERSION} -X github.com/cocoonstack/sandbox-operator/internal/version.gitSHA=${GIT_SHA} -X github.com/cocoonstack/sandbox-operator/internal/version.buildDate=${BUILD_DATE}" \
    -o /sandbox-operator ./cmd/sandbox-operator

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=builder /sandbox-operator /sandbox-operator
ENTRYPOINT ["/sandbox-operator"]
