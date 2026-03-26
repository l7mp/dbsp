###########
# Build the manager binary (monorepo-aware)
FROM docker.io/golang:1.26-alpine AS builder

WORKDIR /workspace

# Copy module manifests first to maximize build cache reuse.
COPY dcontroller/go.mod dcontroller/go.sum /workspace/dcontroller/

# Copy local replacement modules referenced by dcontroller/go.mod.
COPY engine /workspace/engine
COPY connectors/kubernetes /workspace/connectors/kubernetes
COPY connectors/misc /workspace/connectors/misc

WORKDIR /workspace/dcontroller
RUN go mod download

# Copy the dcontroller sources.
COPY dcontroller /workspace/dcontroller

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /workspace/bin/dctl .

###########
# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/bin/dctl .
USER 65532:65532

ENTRYPOINT ["/dctl", "start"]
