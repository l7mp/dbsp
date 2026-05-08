###########
# Build the dbsp binary (monorepo-aware). The dcontroller operator runs as a
# DBSP JavaScript program inside the dbsp runtime; the dctl binary has been
# retired.
FROM docker.io/golang:1.26-alpine AS builder

WORKDIR /workspace

# Copy module manifests first to maximize build cache reuse.
COPY js/go.mod js/go.sum /workspace/js/

# Copy local replacement modules referenced by js/go.mod.
COPY engine /workspace/engine
COPY connectors/kubernetes /workspace/connectors/kubernetes
COPY connectors/misc /workspace/connectors/misc

WORKDIR /workspace/js
RUN go mod download

# Copy the js sources.
COPY js /workspace/js

# Copy the dcontroller JavaScript manager sources.
COPY dcontroller/dcontroller.js /workspace/dcontroller/dcontroller.js
COPY dcontroller/lib /workspace/dcontroller/lib

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /workspace/bin/dbsp ./cmd

###########
# Use distroless as the minimal base image to package the dbsp binary.
# Refer to https://github.com/GoogleContainerTools/distroless for more details.
FROM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/bin/dbsp .
COPY --from=builder /workspace/dcontroller /dcontroller
USER 65532:65532

ENTRYPOINT ["/dbsp"]
