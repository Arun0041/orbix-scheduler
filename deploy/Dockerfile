# syntax=docker/dockerfile:1
# build: static orbixd binary (pure-Go sqlite, so CGO can stay off)
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /out/orbixd ./cmd/orbixd

# runtime: jobs execute through /bin/sh, so busybox instead of scratch
FROM alpine:3.20
RUN adduser -D -u 1000 orbix \
 && mkdir /data && chown orbix:orbix /data
USER orbix
COPY --from=build /out/orbixd /usr/local/bin/orbixd
EXPOSE 8080
ENTRYPOINT ["orbixd"]
