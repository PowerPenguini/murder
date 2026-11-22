# The anthropic plugin's image, built from the repository's root, with
# the manifest the plugin prints as the label Murder reads it from:
#
#   docker build -f examples/plugins/anthropic/Dockerfile \
#     --label "io.murder.plugin.manifest=$(go run ./examples/plugins/anthropic -manifest)" \
#     -t ghcr.io/acme/anthropic-plugin:1.0.0 .
#
# The image holds nothing but the program, and runs as nobody.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY pluginsdk ./pluginsdk
COPY examples/plugins/anthropic ./examples/plugins/anthropic
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /plugin ./examples/plugins/anthropic

FROM scratch
COPY --from=build /plugin /plugin
USER 65534:65534
ENTRYPOINT ["/plugin"]
