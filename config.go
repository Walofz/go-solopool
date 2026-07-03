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
	DiscordWebHook string
	WalletAddress  string
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