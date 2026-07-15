package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

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

func sendNtfyAlert(ntfyURL, subject, message string) {
	if ntfyURL == "" {
		return
	}
	parsedURL, err := url.Parse(ntfyURL)
	if err != nil {
		return
	}
	payload := map[string]string{"title": subject, "message": message}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequest("POST", parsedURL.String(), bytes.NewBuffer(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if parsedURL.User != nil {
		username := parsedURL.User.Username()
		password, _ := parsedURL.User.Password()
		req.SetBasicAuth(username, password)
	}
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

func sendNotification(cfg Config, message string) {
	subject := "Solo Mining Proxy"
	discordMsg := message
	ntfyMsg := "Solo mining proxy started"

	switch strings.ToLower(cfg.NotifyProvider) {
	case "discord":
		sendDiscordAlert(cfg.DiscordWebHook, discordMsg)
	case "ntfy":
		sendNtfyAlert(cfg.NtfyUrl, subject, ntfyMsg)
	default:
		if cfg.DiscordWebHook != "" {
			sendDiscordAlert(cfg.DiscordWebHook, discordMsg)
			return
		}
		if cfg.NtfyUrl != "" {
			sendNtfyAlert(cfg.NtfyUrl, subject, ntfyMsg)
		}
	}
}

func sendBlockFoundNotification(cfg Config, height uint32, diffShare float64, minerID string, wallet string) {
	discordTitle := "🎉 พบบล็อกใหม่แล้ว! (Solo Mining)"
	ntfyTitle := "บล็อกใหม่"
	discordMsg := fmt.Sprintf("🎉 พบบล็อกใหม่แล้ว! Height: #%d | Share Diff: %s | Miner: %s | Wallet: %s", height, formatKMGT(diffShare), minerID, wallet)
	ntfyMsg := fmt.Sprintf("บล็อก #%d ที่ความยาก %s ด้วยเครื่อง %s", height, formatKMGT(diffShare), minerID)

	switch strings.ToLower(cfg.NotifyProvider) {
	case "discord":
		sendBlockFoundAlert(cfg.DiscordWebHook, height, diffShare, minerID, wallet)
	case "ntfy":
		sendNtfyAlert(cfg.NtfyUrl, ntfyTitle, ntfyMsg)
	default:
		if cfg.DiscordWebHook != "" {
			sendBlockFoundAlert(cfg.DiscordWebHook, height, diffShare, minerID, wallet)
			return
		}
		if cfg.NtfyUrl != "" {
			sendNtfyAlert(cfg.NtfyUrl, ntfyTitle, ntfyMsg)
		}
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