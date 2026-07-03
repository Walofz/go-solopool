package main

import (
	"bufio"
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
)

//go:embed index.html
var indexHTML string

type DashboardData struct {
	Wallet      string
	NetDiff     string
	FixedDiff   int
	MinersCount int
	Shares      int
	Blocks      int
	Uptime      string
}

type Miner struct {
	conn       net.Conn
	id         string
	extraNonce string
	asicBoost  bool
}

type StratumJob struct {
	JobID          string
	PrevHashHex    string
	Version        uint32
	BitsHex        string
	CurTime        string
	Height         uint32
	CoinbaseValue  int64
	TxHashes       []string
	TxData         []string
	MerkleBranches []string
	Coinb1         string
	Coinb2         string
}

type JobManager struct {
	sync.RWMutex
	config         Config
	jobs           map[string]*StratumJob
	currentJob     *StratumJob
	clients        map[string]*Miner
	jobTriggerChan chan bool
	jobIDCounter   int

	TotalShares int
	BlocksFound int
	StartTime   time.Time
	NetworkDiff float64
}

func (jm *JobManager) listenZMQ() {
	sub := zmq4.NewSub(context.Background())
	defer sub.Close()

	if err := sub.Dial(jm.config.ZMQUrl); err != nil {
		log.Fatalf("เชื่อมต่อ ZMQ ล้มเหลว: %v", err)
	}
	if err := sub.SetOption(zmq4.OptionSubscribe, "hashblock"); err != nil {
		log.Fatalf("Subscribe hashblock ล้มเหลว: %v", err)
	}

	log.Println("📡 ZMQ: เชื่อมต่อสำเร็จ รอรับข้อมูลบล็อกใหม่...")

	for {
		_, err := sub.Recv()
		if err == nil {
			jm.jobTriggerChan <- true
		}
	}
}

func (jm *JobManager) templateWorker() {
	jm.fetchBlockTemplate()
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-jm.jobTriggerChan:
			jm.fetchBlockTemplate()
		case <-ticker.C:
			jm.fetchBlockTemplate()
		}
	}
}

func (jm *JobManager) handleMiner(conn net.Conn) {
	minerID := conn.RemoteAddr().String()
	extranonce1 := fmt.Sprintf("%08x", time.Now().UnixNano()&0xffffffff)
	miner := &Miner{conn: conn, id: minerID, extraNonce: extranonce1, asicBoost: false}

	jm.Lock()
	jm.clients[minerID] = miner
	jm.Unlock()

	defer func() {
		conn.Close()
		jm.Lock()
		delete(jm.clients, minerID)
		jm.Unlock()
		log.Printf("🔌 เครื่องขุดตัดการเชื่อมต่อ: %s", minerID)
	}()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()

		var msg map[string]interface{}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			log.Printf("❌ รูปแบบ JSON ไม่ถูกต้องจาก Miner: %v", err)
			break
		}

		method, _ := msg["method"].(string)
		idJSON, _ := json.Marshal(msg["id"])
		idStr := string(idJSON)

		switch method {
		case "mining.configure":
			miner.asicBoost = true
			resp := fmt.Sprintf(`{"id":%s,"result":{"version-rolling":true,"version-rolling.mask":"1fffe000"},"error":null}`+"\n", idStr)
			conn.Write([]byte(resp))

		case "mining.extranonce.subscribe":
			resp := fmt.Sprintf(`{"id":%s,"result":true,"error":null}`+"\n", idStr)
			conn.Write([]byte(resp))

		case "mining.subscribe":
			resp := fmt.Sprintf(`{"id":%s,"result":[[["mining.set_difficulty","%s"],["mining.notify","%s"]],"%s",4],"error":null}`+"\n", idStr, minerID, minerID, extranonce1)
			conn.Write([]byte(resp))

		case "mining.authorize":
			authResp := fmt.Sprintf(`{"id":%s,"result":true,"error":null}`+"\n", idStr)
			conn.Write([]byte(authResp))

			diffResp := fmt.Sprintf(`{"id":null,"method":"mining.set_difficulty","params":[%d]}`+"\n", jm.config.FixedDiff)
			conn.Write([]byte(diffResp))

			jm.sendJobToMiner(miner)

		case "mining.submit":
			params, ok := msg["params"].([]interface{})
			if !ok || len(params) < 5 {
				continue
			}

			jobIDStr, _ := params[1].(string)
			extranonce2, _ := params[2].(string)
			ntime, _ := params[3].(string)
			nonce, _ := params[4].(string)

			jm.RLock()
			job, exists := jm.jobs[jobIDStr]
			jm.RUnlock()

			if !exists {
				submitResp := fmt.Sprintf(`{"id":%s,"result":false,"error":[21,"Job not found",null]}`+"\n", idStr)
				conn.Write([]byte(submitResp))
				continue
			}

			jm.Lock()
			jm.TotalShares++
			jm.Unlock()

			version := job.Version
			if len(params) > 5 {
				if versionBitsStr, ok := params[5].(string); ok && versionBitsStr != "" {
					vBits, err := strconv.ParseUint(versionBitsStr, 16, 32)
					if err == nil {
						mask := uint32(0x1fffe000)
						version = (version & ^mask) | (uint32(vBits) & mask)
					}
				}
			}

			coinbaseTxHex := job.Coinb1 + extranonce1 + extranonce2 + job.Coinb2
			coinbaseHash := doubleSHA256(mustDecodeHex(coinbaseTxHex))
			merkleRoot := calculateMerkleRoot(coinbaseHash, job.MerkleBranches)

			headerBytes := buildBlockHeader(version, job.PrevHashHex, merkleRoot, ntime, job.BitsHex, nonce)
			hashBytes := doubleSHA256(headerBytes)
			hashHex := hex.EncodeToString(reverseBytes(hashBytes))

			networkTarget := bitsToTarget(job.BitsHex)
			shareHash := new(big.Int)
			shareHash.SetString(hashHex, 16)

			diffNetwork := targetToDiff(networkTarget)
			diffShare := targetToDiff(shareHash)
			percentToBlock := (diffShare / diffNetwork) * 100

			boostStatus := "OFF"
			if miner.asicBoost {
				boostStatus = "ON"
			}
			log.Printf("⚙️ [Miner: %s (AsicBoost: %s)] ส่ง Share (Diff: %s) | สำเร็จ %.4f%% ของบล็อก", minerID, boostStatus, formatKMGT(diffShare), percentToBlock)

			if shareHash.Cmp(networkTarget) <= 0 {
				jm.Lock()
				jm.BlocksFound++
				jm.Unlock()

				alertMsg := fmt.Sprintf("🎉 พบบล็อกใหม่แล้ว! | เลขบล็อก: #%d | (Share Diff: %s)", job.Height, formatKMGT(diffShare))
				log.Println(alertMsg)
				sendBlockFoundAlert(jm.config.DiscordWebHook, job.Height, diffShare, minerID, jm.config.WalletAddress)

				txCount := encodeVarInt(uint64(len(job.TxHashes) + 1))
				blockHex := hex.EncodeToString(headerBytes) + txCount + coinbaseTxHex
				for _, txHex := range job.TxData {
					blockHex += txHex
				}

				jm.submitBlockToNode(blockHex)
			}

			submitResp := fmt.Sprintf(`{"id":%s,"result":true,"error":null}`+"\n", idStr)
			conn.Write([]byte(submitResp))

		default:
			resp := fmt.Sprintf(`{"id":%s,"result":true,"error":null}`+"\n", idStr)
			conn.Write([]byte(resp))
		}
	}
}

func (jm *JobManager) sendJobToMiner(m *Miner) {
	jm.RLock()
	job := jm.currentJob
	jm.RUnlock()

	if job == nil {
		return
	}

	merkleBranchesJSON, _ := json.Marshal(job.MerkleBranches)
	versionHex := fmt.Sprintf("%08x", job.Version)

	stratumPrevHash := makeStratumPrevHash(job.PrevHashHex)

	jobNotify := fmt.Sprintf(`{"id":null,"method":"mining.notify","params":["%s","%s","%s","%s",%s,"%s","%s","%s",true]}`+"\n",
		job.JobID, stratumPrevHash, job.Coinb1, job.Coinb2, string(merkleBranchesJSON), versionHex, job.BitsHex, job.CurTime)
	m.conn.Write([]byte(jobNotify))
}

func (jm *JobManager) broadcastNewJob() {
	jm.RLock()
	defer jm.RUnlock()

	if len(jm.clients) > 0 {
		log.Printf("🔄 กระจายงานใหม่ให้ %d เครื่องขุด (Job ID: %x)", len(jm.clients), jm.jobIDCounter)
	}

	for _, miner := range jm.clients {
		go jm.sendJobToMiner(miner)
	}
}

func (jm *JobManager) startWebServer() {
	tmpl := template.Must(template.New("index").Parse(indexHTML))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		jm.RLock()
		data := DashboardData{
			Wallet:      jm.config.WalletAddress,
			NetDiff:     formatKMGT(jm.NetworkDiff),
			FixedDiff:   jm.config.FixedDiff,
			MinersCount: len(jm.clients),
			Shares:      jm.TotalShares,
			Blocks:      jm.BlocksFound,
			Uptime:      time.Since(jm.StartTime).Round(time.Second).String(),
		}
		jm.RUnlock()

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmpl.Execute(w, data); err != nil {
			log.Printf("Template error: %v", err)
		}
	})

	if err := http.ListenAndServe(jm.config.WebPort, nil); err != nil {
		log.Fatalf("ไม่สามารถเปิด Web Server ได้: %v", err)
	}
}