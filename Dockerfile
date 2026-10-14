FROM golang:1.27-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dispatcher ./cmd/dispatcher

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out/dispatcher /app/dispatcher
COPY configs/config.example.yaml /app/configs/config.yaml
EXPOSE 8888
ENTRYPOINT ["/app/dispatcher", "-config", "/app/configs/config.yaml"]
