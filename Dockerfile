FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY go.mod *.go *.html ./

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -o proxy *.go

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/proxy .

EXPOSE 3333-3339
CMD ["./proxy"]
