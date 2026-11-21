# The image of one of Murder's plugins, PLUGIN (openai or here), built from
# the repository's root, with the manifest the plugin prints as the label
# Murder reads it from:
#
#   docker build -f plugins/Dockerfile --build-arg PLUGIN=openai \
#     --label "io.murder.plugin.manifest=$(go run ./plugins/openai -manifest)" \
#     -t ghcr.io/powerpenguini/murder-openai:dev .
#
# The program is built for the platform the image is for, on the one that
# builds it: one build makes the images for amd64 and arm64 (see
# .github/workflows/release.yml). The image holds nothing but the program,
# and runs as nobody. Go's image is pinned to its content.
FROM --platform=$BUILDPLATFORM golang:1.26@sha256:4165ee6bf3b7811fd7b40373290405c83d0167dcb87ee5f29c427cdcef45ff8f AS build
ARG TARGETOS
ARG TARGETARCH
ARG PLUGIN
WORKDIR /src
COPY go.mod go.sum ./
COPY pluginsdk ./pluginsdk
COPY plugins ./plugins
RUN test -n "$PLUGIN" && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags=-s -o /plugin ./plugins/$PLUGIN

FROM scratch
COPY --from=build /plugin /plugin
USER 65534:65534
ENTRYPOINT ["/plugin"]
