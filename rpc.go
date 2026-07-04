package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

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
		Timeout:   5 * time.Second,
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

	if rpcResp.Error != nil {
		log.Printf("❌ Node ส่ง Error กลับมา: %v", rpcResp.Error)
		return
	}

	var jobData map[string]interface{}
	if err := json.Unmarshal(rpcResp.Result, &jobData); err != nil || jobData == nil {
		log.Printf("❌ ข้อมูล Result ว่างเปล่าหรืออ่านไม่ได้")
		return
	}

	// ตรวจสอบข้อมูลที่สำคัญว่ามีอยู่จริงและถูกต้อง
	heightFloat, ok1 := jobData["height"].(float64)
	cbValueFloat, ok2 := jobData["coinbasevalue"].(float64)
	prevHash, ok3 := jobData["previousblockhash"].(string)
	versionFloat, ok4 := jobData["version"].(float64)
	bitsHex, ok5 := jobData["bits"].(string)
	curTimeFloat, ok6 := jobData["curtime"].(float64)

	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 {
		log.Printf("❌ Block template ข้อมูลไม่ครบถ้วน (Node อาจยังไม่พร้อมหรือกำลัง Sync)")
		return
	}

	jm.Lock()
	jm.jobIDCounter++
	jobIDStr := fmt.Sprintf("%x", jm.jobIDCounter)

	height := uint32(heightFloat)
	coinbaseValue := int64(cbValueFloat)

	var txHashes []string
	var txData []string
	if txs, ok := jobData["transactions"].([]interface{}); ok {
		for _, tx := range txs {
			if txMap, ok := tx.(map[string]interface{}); ok {
				hash, hashOk := txMap["hash"].(string)
				data, dataOk := txMap["data"].(string)
				if hashOk && dataOk {
					txHashes = append(txHashes, hash)
					txData = append(txData, data)
				}
			}
		}
	}

	merkleBranches := buildMerkleBranches(txHashes)
	coinb1, coinb2 := buildCoinbaseParts(height, coinbaseValue, jm.config.WalletAddress)

	newJob := &StratumJob{
		JobID:          jobIDStr,
		PrevHashHex:    prevHash,
		Version:        uint32(versionFloat),
		BitsHex:        bitsHex,
		CurTime:        fmt.Sprintf("%08x", int(curTimeFloat)),
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
	jm.NetworkDiff = targetToDiff(bitsToTarget(bitsHex))

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
		Timeout:   5 * time.Second,
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
