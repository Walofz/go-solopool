# Go SoloPool

ระบบ Solo Mining Pool ประสิทธิภาพสูงและน้ำหนักเบา เขียนด้วยภาษา Go ออกแบบมาสำหรับเหรียญอัลกอริทึม SHA256d โดยเฉพาะ เพื่อเชื่อมต่อเครื่องขุด ASIC เข้ากับ Node โดยตรงสำหรับการขุดแบบ Solo

## คุณสมบัติหลัก

* **Stratum Protocol Support:** รองรับการเชื่อมต่อกับเครื่องขุด ASIC ยุคใหม่ได้อย่างสมบูรณ์
* **AsicBoost (Version-Rolling):** รองรับการทำงาน AsicBoost เต็มรูปแบบเพื่อประสิทธิภาพการประหยัดพลังงานสูงสุด
* **ZeroMQ (ZMQ) Integration:** เชื่อมต่อรับข้อมูล `hashblock` จาก Node ทันทีเมื่อเกิดบล็อกใหม่ ช่วยลดอัตราการเกิดบล็อกกำพร้า (Orphan Rate)
* **Ultra-Low Latency:** ปรับแต่งโครงสร้างโค้ดระดับไมโคร (Zero Allocation, Pre-computed Payload) เพื่อตอบสนองเครื่องขุดได้ไวที่สุด 
* **Discord Webhook Notifications:** ระบบแจ้งเตือนผ่าน Discord ทันทีเมื่อพบบล็อกใหม่ หรือเมื่อระบบเริ่มทำงาน
* **Web Dashboard:** หน้าเว็บมอนิเตอร์สถานะความยากเครือข่าย เครื่องขุดที่ออนไลน์ จำนวน Share และบล็อกที่เจอแบบ Real-time
* **Multi-Arch & Docker Ready:** รองรับทั้ง x86_64 (PC/Server) และ ARM64 (Raspberry Pi) พร้อมใช้งานผ่าน Docker Image จาก GitHub Container Registry (GHCR) ทันทีโดยไม่ต้องคอมไพล์เอง

## วิธีการติดตั้งและเริ่มต้นใช้งาน

ระบบได้ถูกสร้างเป็น Docker Image ไว้ล่วงหน้าผ่าน GitHub Actions เพื่อความรวดเร็วและประหยัดทรัพยากรเครื่องของคุณ

1. เข้าไปที่โฟลเดอร์ `docker` ของโปรเจกต์:
```bash
cd docker
```

2. แก้ไขข้อมูลในไฟล์ `docker-compose.yml` ให้ตรงกับการใช้งานของคุณ:
* `image`: ระบุตำแหน่ง GHCR Image ของคุณ (เช่น `ghcr.io/walofz/go-solopool/solo-proxy:latest`)
* `RPC_URL`, `RPC_USER`, `RPC_PASS`, `ZMQ_URL`: ข้อมูลสำหรับเชื่อมต่อ RPC ของ Node
* `WALLET_ADDRESS`: ที่อยู่กระเป๋าเงิน (Base58) สำหรับรับรางวัล
* `DISCORD_WEBHOOK_URL`: (ไม่บังคับ) ลิงก์ Webhook สำหรับรับการแจ้งเตือนใน Discord
* `FIXED_DIFF`: ค่าความยากที่ต้องการตั้งให้เครื่องขุด (ค่าเริ่มต้นคือ `10000`)

3. สั่งดึง Image และเปิดใช้งานระบบผ่าน Docker:
```bash
docker-compose pull
docker-compose up -d
```

## การเชื่อมต่อเครื่องขุด

* **ตั้งค่าที่เครื่องขุด (URL):** `stratum+tcp://<pool-ip>:3333`
* **Miner Username/Password:** สามารถตั้งค่าเป็นอะไรก็ได้ (เช่น `user:pass`) ระบบ Proxy จะจัดการยืนยันตัวตนให้อัตโนมัติ
* **หน้าเว็บแดชบอร์ด:** สามารถเข้าดูสถิติได้ที่ `http://<pool-ip>:8080`

## การตรวจสอบ Log

สามารถดูการส่ง Share และสถานะการทำงานของเครื่องขุดแบบ Real-time ได้ผ่านคำสั่ง:
```bash
docker logs -f solo-proxy
```