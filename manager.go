package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	_ "modernc.org/sqlite"
)

type MinerStatus struct {
	ID         string `json:"id"`
	Online     bool   `json:"online"`
	LastSeen   string `json:"last_seen"`
	ShareCount int    `json:"share_count"`
	AsicBoost  bool   `json:"asic_boost"`
	Difficulty int    `json:"difficulty"`
}

type StatsResponse struct {
	Wallet      string        `json:"wallet"`
	NetDiff     string        `json:"network_diff"`
	FixedDiff   int           `json:"fixed_diff"`
	MinersCount int           `json:"miners_count"`
	Shares      int           `json:"shares"`
	Blocks      int           `json:"blocks"`
	Uptime      string        `json:"uptime"`
	UseVardiff  bool          `json:"use_vardiff"`
	NetworkDiff float64       `json:"network_diff_value"`
	Miners      []MinerStatus `json:"miners"`
	BlocksList  []BlockRecord `json:"blocks_list"`
}

type BlockRecord struct {
	ID        int       `json:"id"`
	Height    uint32    `json:"height"`
	FoundAt   time.Time `json:"found_at"`
	DiffShare float64   `json:"diff_share"`
	MinerID   string    `json:"miner_id"`
}

type Miner struct {
	conn       net.Conn
	id         string
	extraNonce string
	asicBoost  bool
	diff       int
	shareTimes []time.Time
	lastSeen   time.Time
	shareCount int
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
	db          *sql.DB
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
	miner := &Miner{conn: conn, id: minerID, extraNonce: extranonce1, asicBoost: false, diff: jm.config.FixedDiff, lastSeen: time.Now()}

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
	// increase buffer to handle large stratum messages (coinbase, job payloads)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024) // allow up to 1MB tokens
	for scanner.Scan() {
		line := scanner.Text()

		var msg map[string]interface{}
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			log.Printf("❌ รูปแบบ JSON ไม่ถูกต้องจาก Miner: %v", err)
			break
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Scanner error from %s: %v", minerID, err)
		}

		method, _ := msg["method"].(string)
		idJSON, _ := json.Marshal(msg["id"])
		idStr := string(idJSON)

		switch method {
		case "mining.configure":
			miner.asicBoost = true
			resp := fmt.Sprintf(`{"id":%s,"result":{"version-rolling":true,"version-rolling.mask":"1fffe000","version-rolling.max-diff":true},"error":null}`+"\n", idStr)
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

			jm.sendDifficultyToMiner(miner)
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
			miner.shareCount++
			miner.lastSeen = time.Now()
			jm.Unlock()

			if jm.config.UseVardiff {
				jm.updateMinerVardiff(miner)
			}

			version := job.Version
			if versionBits, ok := parseVersionBits(params); ok {
				mask := uint32(0x1fffe000)
				version = (version & ^mask) | (uint32(versionBits) & mask)
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
				_ = jm.saveBlockRecord(job.Height, diffShare, minerID)

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

func parseVersionBits(params []interface{}) (uint32, bool) {
	if len(params) <= 5 {
		return 0, false
	}

	switch v := params[5].(type) {
	case string:
		if v == "" {
			return 0, false
		}
		parsed, err := strconv.ParseUint(v, 16, 32)
		if err != nil {
			return 0, false
		}
		return uint32(parsed), true
	case float64:
		return uint32(v), true
	case int:
		return uint32(v), true
	case int64:
		return uint32(v), true
	case uint32:
		return v, true
	case uint64:
		return uint32(v), true
	default:
		return 0, false
	}
}

func calculateVardiff(currentDiff int, shareRate, targetRate float64, minDiff, maxDiff int) int {
	if shareRate <= 0 || targetRate <= 0 {
		return currentDiff
	}

	newDiffF := float64(currentDiff) * (shareRate / targetRate)
	newDiff := int(newDiffF + 0.5) // round to nearest
	if newDiff < minDiff {
		newDiff = minDiff
	}
	if newDiff > maxDiff {
		newDiff = maxDiff
	}
	return newDiff
}

func (jm *JobManager) getEffectiveDifficulty(miner *Miner) int {
	if jm.config.UseVardiff && miner != nil {
		return miner.diff
	}
	return jm.config.FixedDiff
}

func (jm *JobManager) sendDifficultyToMiner(miner *Miner) {
	if miner == nil {
		return
	}

	diffValue := jm.getEffectiveDifficulty(miner)
	resp := fmt.Sprintf(`{"id":null,"method":"mining.set_difficulty","params":[%d]}`+"\n", diffValue)
	_, _ = miner.conn.Write([]byte(resp))
}

func (jm *JobManager) updateMinerVardiff(miner *Miner) {
	miner.shareTimes = append(miner.shareTimes, time.Now())
	window := jm.config.VardiffWindow
	if window <= 0 {
		window = 30
	}
	if len(miner.shareTimes) > window {
		miner.shareTimes = miner.shareTimes[len(miner.shareTimes)-window:]
	}
	if len(miner.shareTimes) < 2 {
		return
	}

	oldest := miner.shareTimes[0]
	latest := miner.shareTimes[len(miner.shareTimes)-1]
	if latest.Sub(oldest) <= 0 {
		return
	}

	shareRate := float64(len(miner.shareTimes)) / latest.Sub(oldest).Seconds()
	targetRate := jm.config.VardiffTarget
	if targetRate <= 0 {
		targetRate = 4.0
	}
	minDiff := jm.config.VardiffMinDiff
	if minDiff <= 0 {
		minDiff = 64
	}
	maxDiff := jm.config.VardiffMaxDiff
	if maxDiff <= 0 {
		maxDiff = 10000
	}
	newDiff := calculateVardiff(miner.diff, shareRate, targetRate, minDiff, maxDiff)
	miner.diff = newDiff

	jm.sendDifficultyToMiner(miner)
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

	jm.sendDifficultyToMiner(m)

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

func (jm *JobManager) initDB() error {
	db, err := sql.Open("sqlite", jm.config.DBPath)
	if err != nil {
		return err
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			height INTEGER NOT NULL,
			found_at DATETIME NOT NULL,
			diff_share REAL NOT NULL,
			miner_id TEXT NOT NULL
		)
	`); err != nil {
		db.Close()
		return err
	}

	jm.db = db
	return nil
}

func (jm *JobManager) saveBlockRecord(height uint32, diffShare float64, minerID string) error {
	if jm.db == nil {
		return nil
	}

	_, err := jm.db.Exec(`
		INSERT INTO blocks (height, found_at, diff_share, miner_id) VALUES (?, ?, ?, ?)
	`, height, time.Now().UTC().Format(time.RFC3339), diffShare, minerID)
	return err
}

func (jm *JobManager) loadBlockRecords() []BlockRecord {
	if jm.db == nil {
		return nil
	}

	rows, err := jm.db.Query(`
		SELECT id, height, found_at, diff_share, miner_id FROM blocks ORDER BY found_at DESC LIMIT 50
	`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var records []BlockRecord
	for rows.Next() {
		var rec BlockRecord
		var foundAt string
		if err := rows.Scan(&rec.ID, &rec.Height, &foundAt, &rec.DiffShare, &rec.MinerID); err != nil {
			continue
		}
		if ts, err := time.Parse(time.RFC3339, foundAt); err == nil {
			rec.FoundAt = ts
		}
		records = append(records, rec)
	}
	return records
}

func (jm *JobManager) minerStatuses() []MinerStatus {
	statuses := make([]MinerStatus, 0, len(jm.clients))
	for _, miner := range jm.clients {
		status := MinerStatus{
			ID:         miner.id,
			Online:     true,
			LastSeen:   miner.lastSeen.UTC().Format(time.RFC3339),
			ShareCount: miner.shareCount,
			AsicBoost:  miner.asicBoost,
			Difficulty: miner.diff,
		}
		statuses = append(statuses, status)
	}
	return statuses
}

func (jm *JobManager) buildStatsResponse() StatsResponse {
	jm.RLock()
	defer jm.RUnlock()

	blocksList := jm.loadBlockRecords()
	return StatsResponse{
		Wallet:      jm.config.WalletAddress,
		NetDiff:     formatKMGT(jm.NetworkDiff),
		FixedDiff:   jm.config.FixedDiff,
		MinersCount: len(jm.clients),
		Shares:      jm.TotalShares,
		Blocks:      jm.BlocksFound,
		Uptime:      time.Since(jm.StartTime).Round(time.Second).String(),
		UseVardiff:  jm.config.UseVardiff,
		NetworkDiff: jm.NetworkDiff,
		Miners:      jm.minerStatuses(),
		BlocksList:  blocksList,
	}
}

func (jm *JobManager) startWebServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		stats := jm.buildStatsResponse()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Printf("Encode stats error: %v", err)
		}
	})

	http.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := jm.buildStatsResponse()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			log.Printf("Encode stats error: %v", err)
		}
	})

	http.HandleFunc("/api/miners", func(w http.ResponseWriter, r *http.Request) {
		jm.RLock()
		miners := jm.minerStatuses()
		jm.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(miners); err != nil {
			log.Printf("Encode miners error: %v", err)
		}
	})

	if err := http.ListenAndServe(jm.config.WebPort, nil); err != nil {
		log.Fatalf("ไม่สามารถเปิด Web Server ได้: %v", err)
	}
}
