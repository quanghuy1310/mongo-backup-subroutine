package main

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	MongoURI      string
	BackupPath    string
	Compression   string
	RetryInterval time.Duration
	MaxRetries    int
}

var AppConfig Config

func LoadConfig() {
	err := godotenv.Load()
	if err != nil {
		log.Println("Can't load .env file, using environment variables")
	}

	retryMin, _ := strconv.Atoi(os.Getenv("RETRY_INTERVAL_MIN"))

	AppConfig = Config{
		MongoURI:      os.Getenv("MONGO_URI"),
		BackupPath:    os.Getenv("BACKUP_PATH"),
		Compression:   os.Getenv("COMPRESSION"),
		RetryInterval: time.Duration(retryMin) * time.Minute,
		MaxRetries:    atoiDefault(os.Getenv("MAX_RETRIES"), 5),
	}
}

func atoiDefault(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}
