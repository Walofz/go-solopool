package main

import (
	"log"
	"net"
	"time"
)

func main() {
	cfg := loadConfig()

	if cfg.WalletAddress == "" {
		log.Fatal("กรุณาระบุ WALLET_ADDRESS ในไฟล์ docker-compose.yml")
	}

	manager := &JobManager{
		config:         cfg,
		clients:        make(map[string]*Miner),
		jobs:           make(map[string]*StratumJob),
		jobTriggerChan: make(chan bool, 1),
		StartTime:      time.Now(),
	}

	if err := manager.initDB(); err != nil {
		log.Fatalf("ไม่สามารถเปิดฐานข้อมูล SQLite ได้: %v", err)
	}

	sendDiscordAlert(cfg.DiscordWebHook, "🚀 ระบบ Solo Mining Proxy (AsicBoost Enabled) เริ่มทำงานแล้ว")

	go manager.listenZMQ()
	go manager.templateWorker()
	go manager.startWebServer()

	listener, err := net.Listen("tcp", cfg.StratumPort)
	if err != nil {
		log.Fatalf("ไม่สามารถเปิดพอร์ต Stratum ได้: %v", err)
	}
	defer listener.Close()

	log.Printf("Stratum Server ทำงานบนพอร์ต %s (Fixed Diff: %d)", cfg.StratumPort, cfg.FixedDiff)
	log.Printf("ขุดเข้ากระเป๋า: %s", cfg.WalletAddress)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("ข้อผิดพลาดในการรับการเชื่อมต่อ: %v", err)
			continue
		}
		go manager.handleMiner(conn)
	}
}
