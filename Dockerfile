FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /node ./cmd/node && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -o /client ./cmd/client && \
    mkdir -p /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /node /app/shardzero-node
COPY --from=build /client /app/shardzero-client
COPY --from=build --chown=65532:65532 /data /data
COPY configs/docker.json /app/cluster.json
WORKDIR /data
EXPOSE 7001
ENTRYPOINT ["/app/shardzero-node"]
CMD ["--listen=0.0.0.0:7001", "--data=/data/node-1"]
