# Gateway (and the load generator, for in-cluster tests). Static binaries on distroless.
# Cross-compiles on the build platform, so multi-arch images don't run Go under emulation.
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETOS=linux TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" -o /out/gateway ./cmd/gateway \
 && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" -o /out/loadgen ./cmd/loadgen

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /out/loadgen /
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/gateway"]
