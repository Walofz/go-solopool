package main

import (
        "bufio"
        "bytes"
        "context"
        "crypto/sha256"
        "encoding/binary"
        "encoding/hex"
        "encoding/json"
        "fmt"
        "log"
        "math/big"
        "net"
        "net/http"
        "os"
        "strconv"
        "strings"
        "sync"
        "time"

        "github.com/go-zeromq/zmq4"
)

type Config struct {
        RPCUrl         string
        RPCUser        string
        RPCPass        string
        ZMQUrl         string
        StratumPort    string
        WebPort        string
        FixedDiff      int
        DiscordWebHook string
        WalletAddress  string
}

type RPCRequest struct {
        JSONRPC string        `json:"jsonrpc"`
        Method  string        `json:"method"`
        Params  []interface{} `json:"params"`
        ID      int           `json:"id"`
}

type RPCResponse struct {
        Result json.RawMessage `json:"result"`
        Error  interface{}     `json:"error"`
        ID     int             `json:"id"`
}

type Miner struct {
        conn       net.Conn
        id         string
        extraNonce string
        asicBoost  bool // ตัวแปรเก็บสถานะ AsicBoost ของเครื่องนี้
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

func loadConfig() Config {
        diff, _ := strconv.Atoi(getEnv("FIXED_DIFF", "10000"))
        return Config{
                RPCUrl:         getEnv("RPC_URL", "http://127.0.0.1:13031"),
                RPCUser:        getEnv("RPC_USER", "user"),
                RPCPass:        getEnv("RPC_PASS", "pass"),
                ZMQUrl:         getEnv("ZMQ_URL", "tcp://127.0.0.1:28332"),
                StratumPort:    getEnv("STRATUM_PORT", ":3333"),
                WebPort:        getEnv("WEB_PORT", ":8080"),
                FixedDiff:      diff,
                DiscordWebHook: getEnv("DISCORD_WEBHOOK_URL", ""),
                WalletAddress:  getEnv("WALLET_ADDRESS", ""),
        }
}

func getEnv(key, fallback string) string {
        if value, exists := os.LookupEnv(key); exists {
                return value
        }
        return fallback
}

func sendDiscordAlert(webhookURL, message string) {
        if webhookURL == "" {
                return
        }
        payload := map[string]string{"content": message}
        body, _ := json.Marshal(payload)
        resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(body))
        if err == nil {
                resp.Body.Close()
        }
}

func sendBlockFoundAlert(webhookURL string, height uint32, diffShare float64, minerID string, wallet string) {
	if webhookURL == "" {
		return
	}

	embed := map[string]interface{}{
		"title":       "🎉 พบบล็อกใหม่แล้ว! (Solo Mining)",
		"description": "ระบบรับแชร์ที่ถูกต้องและทำการส่งบล็อกเข้าสู่โหนดเรียบร้อยแล้ว",
		"color":       16766720,
		"fields": []map[string]interface{}{
			{"name": "📦 เลขบล็อก (Height)", "value": fmt.Sprintf("`#%d`", height), "inline": true},
			{"name": "⛏️ ความยาก (Share Diff)", "value": fmt.Sprintf("`%s`", formatKMGT(diffShare)), "inline": true},
			{"name": "🖥️ เครื่องขุดที่พบ", "value": fmt.Sprintf("`%s`", minerID), "inline": false},
			{"name": "💼 กระเป๋าปลายทาง", "value": fmt.Sprintf("`%s`", wallet), "inline": false},
		},
		"timestamp": time.Now().Format(time.RFC3339),
	}

	payload := map[string]interface{}{"embeds": []interface{}{embed}}
	body, _ := json.Marshal(payload)
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(body))
	if err == nil {
		resp.Body.Close()
	}
}

func (jm *JobManager) fetchBlockTemplate() {
        reqPayload := RPCRequest{
                JSONRPC: "1.0",
                Method:  "getblocktemplate",
                Params: []interface{}{
                        map[string]interface{}{
                                "rules": []string{"segwit"},
                                "algo":  "sha256d",
                        },
                },
                ID: 1,
        }

        body, _ := json.Marshal(reqPayload)
        client := &http.Client{
                Timeout: 5 * time.Second,
                Transport: &http.Transport{DisableKeepAlives: true},
        }

        var resp *http.Response
        var err error

        for i := 0; i < 3; i++ {
                req, _ := http.NewRequest("POST", jm.config.RPCUrl, bytes.NewBuffer(body))
                req.SetBasicAuth(jm.config.RPCUser, jm.config.RPCPass)
                req.Header.Set("Content-Type", "application/json")
                req.Close = true

                resp, err = client.Do(req)
                if err == nil {
                        break
                }
                time.Sleep(500 * time.Millisecond)
        }

        if err != nil {
                log.Printf("❌ ดึง Block Template ล้มเหลวหลังจากลองใหม่: %v", err)
                return
        }
        defer resp.Body.Close()

        var rpcResp RPCResponse
        if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
                return
        }

        var jobData map[string]interface{}
        json.Unmarshal(rpcResp.Result, &jobData)

        jm.Lock()
        jm.jobIDCounter++
        jobIDStr := fmt.Sprintf("%x", jm.jobIDCounter)

        height := uint32(jobData["height"].(float64))
        coinbaseValue := int64(jobData["coinbasevalue"].(float64))

        var txHashes []string
        var txData []string
        if txs, ok := jobData["transactions"].([]interface{}); ok {
                for _, tx := range txs {
                        txMap := tx.(map[string]interface{})
                        txHashes = append(txHashes, txMap["hash"].(string))
                        txData = append(txData, txMap["data"].(string))
                }
        }

        merkleBranches := buildMerkleBranches(txHashes)
        coinb1, coinb2 := buildCoinbaseParts(height, coinbaseValue, jm.config.WalletAddress)

        newJob := &StratumJob{
                JobID:          jobIDStr,
                PrevHashHex:    jobData["previousblockhash"].(string),
                Version:        uint32(jobData["version"].(float64)),
                BitsHex:        jobData["bits"].(string),
                CurTime:        fmt.Sprintf("%08x", int(jobData["curtime"].(float64))),
                Height:         height,
                CoinbaseValue:  coinbaseValue,
                TxHashes:       txHashes,
                TxData:         txData,
                MerkleBranches: merkleBranches,
                Coinb1:         coinb1,
                Coinb2:         coinb2,
        }

        for k, v := range jm.jobs {
                if v.Height < height-1 {
                        delete(jm.jobs, k)
                }
        }

        jm.jobs[jobIDStr] = newJob
        jm.currentJob = newJob

        if bitsHex, ok := jobData["bits"].(string); ok {
                jm.NetworkDiff = targetToDiff(bitsToTarget(bitsHex))
        }

        log.Printf("📦 ดึง Block Template สำเร็จ | Height: %d | Tx: %d | Diff: %s | Job: %s", height, len(txHashes), formatKMGT(jm.NetworkDiff), jobIDStr)
        jm.Unlock()

        jm.broadcastNewJob()
}

func (jm *JobManager) submitBlockToNode(blockHex string) {
        reqPayload := RPCRequest{
                JSONRPC: "1.0",
                Method:  "submitblock",
                Params:  []interface{}{blockHex},
                ID:      2,
        }

        body, _ := json.Marshal(reqPayload)
        client := &http.Client{
                Timeout: 5 * time.Second,
                Transport: &http.Transport{DisableKeepAlives: true},
        }

        req, _ := http.NewRequest("POST", jm.config.RPCUrl, bytes.NewBuffer(body))
        req.SetBasicAuth(jm.config.RPCUser, jm.config.RPCPass)
        req.Header.Set("Content-Type", "application/json")
        req.Close = true

        resp, err := client.Do(req)
        if err != nil {
                log.Printf("ยิง submitblock ล้มเหลว: %v", err)
                return
        }
        defer resp.Body.Close()

        var rpcResp RPCResponse
        json.NewDecoder(resp.Body).Decode(&rpcResp)
        log.Printf("ผลลัพธ์จาก Node หลังจาก submitblock: %s", string(rpcResp.Result))
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
                        miner.asicBoost = true // บันทึกว่าเครื่องนี้ขอใช้ AsicBoost
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

                        // แสดงสถานะ ON/OFF ตามตัวแปรที่เก็บไว้
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
        http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
                jm.RLock()
                minersCount := len(jm.clients)
                shares := jm.TotalShares
                blocks := jm.BlocksFound
                uptime := time.Since(jm.StartTime).Round(time.Second)
                netDiff := jm.NetworkDiff
                jm.RUnlock()

                html := fmt.Sprintf(`
                <!DOCTYPE html>
                <html lang="th">
                <head>
                        <meta charset="UTF-8">
                        <meta http-equiv="refresh" content="5">
                        <title>Solo Mining Proxy Dashboard</title>
                        <style>
                                body { font-family: Arial, sans-serif; background-color: #f4f4f9; padding: 20px; }
                                .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); max-width: 400px; margin: auto; }
                                h2 { text-align: center; color: #333; }
                                .stat { font-size: 18px; margin: 10px 0; display: flex; justify-content: space-between; }
                                .stat span { font-weight: bold; color: #007bff; }
                        </style>
                </head>
                <body>
                        <div class="card">
                                <h2>Coins Solo Proxy</h2>
                                <div class="stat">ความยากเครือข่าย: <span>%s</span></div>
                                <div class="stat">เครื่องขุดที่ออนไลน์: <span>%d</span></div>
                                <div class="stat">จำนวน Share รวม: <span>%d</span></div>
                                <div class="stat">บล็อกที่ขุดเจอ: <span>%d</span></div>
                                <div class="stat">ระยะเวลาทำงาน: <span>%s</span></div>
                        </div>
                </body>
                </html>
                `, formatKMGT(netDiff), minersCount, shares, blocks, uptime)

                w.Header().Set("Content-Type", "text/html; charset=utf-8")
                w.Write([]byte(html))
        })

        if err := http.ListenAndServe(jm.config.WebPort, nil); err != nil {
                log.Fatalf("ไม่สามารถเปิด Web Server ได้: %v", err)
        }
}

func formatKMGT(val float64) string {
        if val >= 1e18 {
                return fmt.Sprintf("%.2f E", val/1e18)
        }
        if val >= 1e15 {
                return fmt.Sprintf("%.2f P", val/1e15)
        }
        if val >= 1e12 {
                return fmt.Sprintf("%.2f T", val/1e12)
        }
        if val >= 1e9 {
                return fmt.Sprintf("%.2f G", val/1e9)
        }
        if val >= 1e6 {
                return fmt.Sprintf("%.2f M", val/1e6)
        }
        if val >= 1e3 {
                return fmt.Sprintf("%.2f K", val/1e3)
        }
        return fmt.Sprintf("%.2f", val)
}

func doubleSHA256(b []byte) []byte {
        h := sha256.Sum256(b)
        h2 := sha256.Sum256(h[:])
        return h2[:]
}

func reverseBytes(b []byte) []byte {
        reversed := make([]byte, len(b))
        for i := range b {
                reversed[i] = b[len(b)-1-i]
        }
        return reversed
}

func mustDecodeHex(s string) []byte {
        b, _ := hex.DecodeString(s)
        return b
}

func encodeVarInt(val uint64) string {
        if val < 0xfd {
                return fmt.Sprintf("%02x", val)
        } else if val <= 0xffff {
                buf := make([]byte, 2)
                binary.LittleEndian.PutUint16(buf, uint16(val))
                return "fd" + hex.EncodeToString(buf)
        } else if val <= 0xffffffff {
                buf := make([]byte, 4)
                binary.LittleEndian.PutUint32(buf, uint32(val))
                return "fe" + hex.EncodeToString(buf)
        }
        buf := make([]byte, 8)
        binary.LittleEndian.PutUint64(buf, val)
        return "ff" + hex.EncodeToString(buf)
}

func decodeBase58Address(b58 string) ([]byte, error) {
        alphabet := "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        ans := big.NewInt(0)
        radix := big.NewInt(58)
        for i := 0; i < len(b58); i++ {
                idx := strings.IndexByte(alphabet, b58[i])
                if idx == -1 {
                        return nil, fmt.Errorf("invalid base58 character")
                }
                ans.Mul(ans, radix)
                ans.Add(ans, big.NewInt(int64(idx)))
        }
        decoded := ans.Bytes()
        pad := 0
        for i := 0; i < len(b58) && b58[i] == '1'; i++ {
                pad++
        }
        padded := make([]byte, pad+len(decoded))
        copy(padded[pad:], decoded)
        return padded, nil
}

func buildCoinbaseParts(height uint32, reward int64, address string) (string, string) {
        decodedAddr, _ := decodeBase58Address(address)
        pubKeyHash := decodedAddr[1 : len(decodedAddr)-4]

        scriptPubKey := fmt.Sprintf("76a914%s88ac", hex.EncodeToString(pubKeyHash))
        scriptLength := encodeVarInt(uint64(len(scriptPubKey) / 2))

        heightHex := ""
        if height <= 255 {
                heightHex = fmt.Sprintf("01%02x", height)
        } else if height <= 65535 {
                buf := make([]byte, 2)
                binary.LittleEndian.PutUint16(buf, uint16(height))
                heightHex = "02" + hex.EncodeToString(buf)
        } else {
                buf := make([]byte, 4)
                binary.LittleEndian.PutUint32(buf, uint32(height))
                heightHex = "03" + hex.EncodeToString(buf)[:6]
        }

        coinb1 := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff"

        inputScriptLen := encodeVarInt(uint64((len(heightHex)/2) + 8))
        coinb1 += inputScriptLen + heightHex

        rewardBuf := make([]byte, 8)
        binary.LittleEndian.PutUint64(rewardBuf, uint64(reward))

        coinb2 := fmt.Sprintf("ffffffff01%s%s%s00000000", hex.EncodeToString(rewardBuf), scriptLength, scriptPubKey)
        return coinb1, coinb2
}

func buildMerkleBranches(txHashes []string) []string {
        if len(txHashes) == 0 {
                return []string{}
        }
        var steps [][]byte
        for _, h := range txHashes {
                steps = append(steps, reverseBytes(mustDecodeHex(h)))
        }

        var branches []string
        for len(steps) > 0 {
                branches = append(branches, hex.EncodeToString(steps[0]))
                var nextSteps [][]byte
                for i := 1; i < len(steps); i += 2 {
                        if i+1 < len(steps) {
                                nextSteps = append(nextSteps, doubleSHA256(append(steps[i], steps[i+1]...)))
                        } else {
                                nextSteps = append(nextSteps, doubleSHA256(append(steps[i], steps[i]...)))
                        }
                }
                steps = nextSteps
        }
        return branches
}

func calculateMerkleRoot(coinbaseHash []byte, branches []string) []byte {
        root := coinbaseHash
        for _, branchHex := range branches {
                branchBytes := mustDecodeHex(branchHex)
                root = doubleSHA256(append(root, branchBytes...))
        }
        return root
}

func makeStratumPrevHash(rpcHex string) string {
        b := mustDecodeHex(rpcHex)
        bLE := reverseBytes(b)
        for i := 0; i < len(bLE); i += 4 {
                bLE[i], bLE[i+1], bLE[i+2], bLE[i+3] = bLE[i+3], bLE[i+2], bLE[i+1], bLE[i]
        }
        return hex.EncodeToString(bLE)
}

func buildBlockHeader(version uint32, rpcPrevHashHex string, merkleRoot []byte, ntimeHex, bitsHex, nonceHex string) []byte {
        buf := new(bytes.Buffer)

        binary.Write(buf, binary.LittleEndian, version)
        buf.Write(reverseBytes(mustDecodeHex(rpcPrevHashHex)))
        buf.Write(merkleRoot)
        buf.Write(reverseBytes(mustDecodeHex(ntimeHex)))
        buf.Write(reverseBytes(mustDecodeHex(bitsHex)))
        buf.Write(reverseBytes(mustDecodeHex(nonceHex)))

        return buf.Bytes()
}

func bitsToTarget(bitsHex string) *big.Int {
        bits, _ := strconv.ParseUint(bitsHex, 16, 32)
        shift := (bits >> 24) & 0xff
        val := bits & 0x00ffffff
        target := big.NewInt(int64(val))
        target.Lsh(target, uint((shift-3)*8))
        return target
}

func targetToDiff(target *big.Int) float64 {
        maxTarget, _ := new(big.Int).SetString("00000000ffff0000000000000000000000000000000000000000000000000000", 16)
        maxFloat := new(big.Float).SetInt(maxTarget)
        targetFloat := new(big.Float).SetInt(target)
        diff, _ := new(big.Float).Quo(maxFloat, targetFloat).Float64()
        return diff
}
