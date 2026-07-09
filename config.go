package main

import (
	"os"
	"strconv"
)

type Config struct {
	RPCUrl         string
	RPCUser        string
	RPCPass        string
	ZMQUrl         string
	StratumPort    string
	WebPort        string
	FixedDiff      int
	UseVardiff     bool
	VardiffWindow  int
	VardiffTarget  float64
	VardiffMinDiff int
	VardiffMaxDiff int
	DiscordWebHook string
	WalletAddress  string
	DBPath         string
}

func loadConfig() Config {
	diff, _ := strconv.Atoi(getEnv("FIXED_DIFF", "8096"))
	useVardiff := getEnv("USE_VARDIFF", "no") == "yes"
	vardiffWindow, _ := strconv.Atoi(getEnv("VARDIFF_WINDOW", "30"))
	vardiffTarget, _ := strconv.ParseFloat(getEnv("VARDIFF_TARGET", "4"), 64)
	vardiffMinDiff, _ := strconv.Atoi(getEnv("VARDIFF_MIN_DIFF", "64"))
	vardiffMaxDiff, _ := strconv.Atoi(getEnv("VARDIFF_MAX_DIFF", "10000"))

	return Config{
		RPCUrl:         getEnv("RPC_URL", "http://127.0.0.1:13031"),
		RPCUser:        getEnv("RPC_USER", "user"),
		RPCPass:        getEnv("RPC_PASS", "pass"),
		ZMQUrl:         getEnv("ZMQ_URL", "tcp://127.0.0.1:28332"),
		StratumPort:    getEnv("STRATUM_PORT", ":3333"),
		WebPort:        getEnv("WEB_PORT", ":8080"),
		FixedDiff:      diff,
		UseVardiff:     useVardiff,
		VardiffWindow:  vardiffWindow,
		VardiffTarget:  vardiffTarget,
		VardiffMinDiff: vardiffMinDiff,
		VardiffMaxDiff: vardiffMaxDiff,
		DiscordWebHook: getEnv("DISCORD_WEBHOOK_URL", ""),
		WalletAddress:  getEnv("WALLET_ADDRESS", ""),
		DBPath:         getEnv("DB_PATH", "./soloproxy.db"),
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
