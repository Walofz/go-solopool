# Go SoloPool

ระบบ Solo Mining Proxy สำหรับขุดแบบ Solo บนเครือข่ายที่รองรับ Stratum และ RPC โดยมีฟีเจอร์พื้นฐานสำหรับตัวขุด ASIC, SQLite storage และ JSON stats API

## คุณสมบัติหลัก

- Stratum protocol support สำหรับเครื่องขุด ASIC
- AsicBoost / version-rolling support
- ZeroMQ (ZMQ) integration เพื่อรับ hashblock จาก node ทันที
- Discord webhook notification เมื่อเจอบล็อกหรือเริ่มระบบ
- JSON stats API ที่ให้ข้อมูลสถานะและประวัติบล็อกผ่าน HTTP
- SQLite storage สำหรับเก็บประวัติบล็อกแบบถาวร
- Docker ready และใช้กับ docker-compose ได้ทันที

## การติดตั้งและรัน

1. คัดลอกโปรเจกต์ไปยังเครื่องของคุณ
2. แก้ไขค่าตั้งใน docker-compose.yml ให้ตรงกับ node ของคุณ:
   - RPC_URL / RPC_USER / RPC_PASS
   - WALLET_ADDRESS
   - DISCORD_WEBHOOK_URL (ถ้ามี)
   - FIXED_DIFF=8096
3. รันด้วย Docker:

```bash
docker compose up -d --build
```

## ตัวเลือกสิ่งแวดล้อม

- `FIXED_DIFF=8096` : ความยากคงที่ที่ส่งให้เครื่องขุด
- `DB_PATH=./soloproxy.db` : ที่เก็บฐานข้อมูล SQLite

## การเชื่อมต่อเครื่องขุด

- URL: stratum+tcp://<pool-ip>:3333
- Username / Password: ใส่อะไรก็ได้ ระบบจะยอมรับและจัดการ auth เอง

## API

- GET / : คืนข้อมูลสถิติในรูปแบบ JSON
- GET /api/stats : คืนข้อมูลสถิติรวมและประวัติบล็อกในรูปแบบ JSON
- GET /api/miners : คืนสถานะเครื่องขุดแต่ละเครื่องในรูปแบบ JSON

## Docker image

GitHub Actions จะสร้าง Docker image และ push ไปยัง GitHub Container Registry เมื่อมีการ push ไปยังสาขา `main` หรือ `master`.

ตัวอย่าง image tag:

```text
ghcr.io/<OWNER>/go-solo-mining-proxy:latest
```

## ตรวจสอบระบบ

```bash
docker logs -f solo-proxy
```

## ทดสอบ

```bash
go test ./...
```

## รันจากเครื่อง

```bash
go run main.go
```

## สร้าง Docker image

```bash
docker build -t go-solo-mining-proxy .
```
