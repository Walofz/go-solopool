FROM alpine:latest
WORKDIR /app

ARG TARGETARCH

COPY build/proxy-${TARGETARCH} ./proxy
COPY *.html ./

EXPOSE 3333-3339
CMD ["./proxy"]