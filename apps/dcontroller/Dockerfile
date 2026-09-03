# syntax=docker/dockerfile:1
#
# Build context is the workspace root:
#   docker build -f apps/dcontroller/Dockerfile .
#
# Delta-controller is a DBSP JavaScript program: the image ships the `dbsp`
# runtime binary from the js module, the JavaScript stdlib the runtime loads
# modules from, and the dcontroller script tree.

###########
# Build the dbsp runtime binary.
FROM --platform=$BUILDPLATFORM docker.io/golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Module manifests first, so the dependency download layer is cached across
# source edits. The js module resolves engine and the connectors through
# `replace` directives pointing at sibling directories, so their manifests must
# be in place before the module graph can be loaded at all.
COPY engine/go.mod engine/go.sum engine/
COPY connectors/kubernetes/go.mod connectors/kubernetes/go.sum connectors/kubernetes/
COPY connectors/misc/go.mod connectors/misc/go.sum connectors/misc/
COPY connectors/xds/go.mod connectors/xds/go.sum connectors/xds/
COPY js/go.mod js/go.sum js/

WORKDIR /workspace/js
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Sources. No go.work: the js module carries its own replace directives, and a
# workspace file would drag in modules that are not part of this image.
COPY engine /workspace/engine
COPY connectors /workspace/connectors
COPY js /workspace/js

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o /out/dbsp ./cmd

###########
# Distroless runtime image. See
# https://github.com/GoogleContainerTools/distroless for details.
FROM gcr.io/distroless/static:nonroot

# The runtime resolves named require() targets against its JavaScript stdlib,
# which it discovers as <directory of the binary>/../stdlib. DBSP_STDLIB states
# the same location explicitly so the layout survives an entrypoint override.
COPY --from=builder /out/dbsp /usr/local/bin/dbsp
COPY js/stdlib /usr/local/stdlib
ENV DBSP_STDLIB=/usr/local/stdlib

# The dcontroller script tree. dcontroller.js resolves ./lib relative to itself.
COPY apps/dcontroller/dcontroller.js /dcontroller/dcontroller.js
COPY apps/dcontroller/lib /dcontroller/lib

USER 65532:65532

ENTRYPOINT ["/usr/local/bin/dbsp"]
CMD ["/dcontroller/dcontroller.js"]
