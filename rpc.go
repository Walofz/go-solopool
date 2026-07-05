package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

var rpcClient = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	},
}

var getBlockTemplatePayload []byte

func init() {
	req := RPCRequest{
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
	getBlockTemplatePayload, _ = json.Marshal(req)
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

type BlockTemplate struct {
	Height            uint32 `json:"height"`
	CoinbaseValue     int64  `json:"coinbasevalue"`
	PreviousBlockHash string `json:"previousblockhash"`
	Version           uint32 `json:"version"`
	Bits              string `json:"bits"`
	CurTime           uint32 `json:"curtime"`
	Transactions      []struct {
		Hash string `json:"hash"`
		Data string `json:"data"`
	} `json:"transactions"`
}

func (jm *JobManager) fetchBlockTemplate() {
	var resp *http.Response
	var err error

	for i := 0; i < 3; i++ {
		req, errReq := http.NewRequest("POST", jm.config.RPCUrl, bytes.NewReader(getBlockTemplatePayload))
		if errReq != nil {
			log.Printf("❌ เตรียม HTTP Request ล้มเหลว: %v", errReq)
			return
		}

		req.SetBasicAuth(jm.config.RPCUser, jm.config.RPCPass)
		req.Header.Set("Content-Type", "application/json")

		resp, err = rpcClient.Do(req)
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
		log.Printf("❌ อ่านข้อมูลการตอบกลับล้มเหลว: %v", err)
		return
	}

	if rpcResp.Error != nil {
		log.Printf("❌ Node ส่ง Error กลับมา: %v", rpcResp.Error)
		return
	}

	var bt BlockTemplate
	if err := json.Unmarshal(rpcResp.Result, &bt); err != nil {
		log.Printf("❌ แปลงข้อมูล Block template ล้มเหลว (Node อาจกำลัง Sync): %v", err)
		return
	}

	txHashes := make([]string, 0, len(bt.Transactions))
	txData := make([]string, 0, len(bt.Transactions))
	for _, tx := range bt.Transactions {
		txHashes = append(txHashes, tx.Hash)
		txData = append(txData, tx.Data)
	}

	merkleBranches := buildMerkleBranches(txHashes)
	coinb1, coinb2 := buildCoinbaseParts(bt.Height, bt.CoinbaseValue, jm.config.WalletAddress)
	curTimeHex := fmt.Sprintf("%08x", bt.CurTime)

	jm.Lock()
	jm.jobIDCounter++
	jobIDStr := fmt.Sprintf("%x", jm.jobIDCounter)

	newJob := &StratumJob{
		JobID:          jobIDStr,
		PrevHashHex:    bt.PreviousBlockHash,
		Version:        bt.Version,
		BitsHex:        bt.Bits,
		CurTime:        curTimeHex,
		Height:         bt.Height,
		CoinbaseValue:  bt.CoinbaseValue,
		TxHashes:       txHashes,
		TxData:         txData,
		MerkleBranches: merkleBranches,
		Coinb1:         coinb1,
		Coinb2:         coinb2,
	}

	for k, v := range jm.jobs {
		if v.Height < bt.Height-1 {
			delete(jm.jobs, k)
		}
	}

	jm.jobs[jobIDStr] = newJob
	jm.currentJob = newJob
	jm.NetworkDiff = targetToDiff(bitsToTarget(bt.Bits))

	log.Printf("📦 ดึง Block Template สำเร็จ | Height: %d | Tx: %d | Diff: %s | Job: %s", bt.Height, len(txHashes), formatKMGT(jm.NetworkDiff), jobIDStr)
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

	body, err := json.Marshal(reqPayload)
	if err != nil {
		log.Printf("❌ สร้าง Request สำหรับ Submit ล้มเหลว: %v", err)
		return
	}

	req, err := http.NewRequest("POST", jm.config.RPCUrl, bytes.NewReader(body))
	if err != nil {
		log.Printf("❌ เตรียม HTTP Request ล้มเหลว: %v", err)
		return
	}

	req.SetBasicAuth(jm.config.RPCUser, jm.config.RPCPass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := rpcClient.Do(req)
	if err != nil {
		log.Printf("ยิง submitblock ล้มเหลว: %v", err)
		return
	}
	defer resp.Body.Close()

	var rpcResp RPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		log.Printf("อ่านผลลัพธ์ submitblock ล้มเหลว: %v", err)
		return
	}
	log.Printf("ผลลัพธ์จาก Node หลังจาก submitblock: %s", string(rpcResp.Result))
}
