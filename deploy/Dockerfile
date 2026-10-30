FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN CGO_ENABLED=0 go build -buildvcs=false -ldflags="-s -w" -o /out/ ./cmd/rl-adapter ./cmd/rl-load

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /
ENTRYPOINT ["/rl-adapter"]
