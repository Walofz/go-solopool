# กำหนด Base Image เป็น Go สำหรับ Build
FROM golang:1.21-alpine AS builder

WORKDIR /app

# คัดลอกไฟล์โค้ดเข้า Container
COPY main.go .

# สร้างโมดูลและดาวน์โหลด Package ที่จำเป็น (go-zeromq)
RUN go mod init soloproxy && \
    go get github.com/go-zeromq/zmq4 && \
    go mod tidy

# Build โปรแกรม
RUN CGO_ENABLED=0 GOOS=linux go build -o proxy main.go

# ใช้ Image ขนาดเล็กสำหรับรันโปรแกรม
FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/proxy .

# เปิดพอร์ต Stratum
EXPOSE 3333

# รันโปรแกรมเมื่อเริ่ม Container
CMD ["./proxy"]
