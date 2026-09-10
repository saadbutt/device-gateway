# Multi-arch by design: these gateways often run on an amd64 cloud box in one
# deployment and an arm64 board in a depot in the next.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/simulator ./cmd/simulator

# scratch, not alpine: the binary is static and has nothing to talk to.
FROM scratch
COPY --from=build /out/gateway /gateway
COPY --from=build /out/simulator /simulator
EXPOSE 5027 9100
USER 65534:65534
ENTRYPOINT ["/gateway"]
