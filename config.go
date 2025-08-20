package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	MongoURI      string
	BackupPath    string
	MongodumpPath string
	Compression   string
	RetryInterval time.Duration
	MaxRetries    int
	MaxRetryDays  int
	BackupTimeout time.Duration
	KeepRawFiles  bool
}

var AppConfig Config

func LoadConfig() {
	err := godotenv.Load()
	if err != nil {
		logPrint("WARN", "Can't load .env file, using environment variables")
	}

	// Load retry interval (support both formats)
	retryInterval := time.Duration(0)
	if v := os.Getenv("RETRY_INTERVAL"); v != "" {
		retryInterval, err = time.ParseDuration(v)
		if err != nil {
			logPrint("ERROR", fmt.Sprintf("Invalid RETRY_INTERVAL: %v", err))
			retryInterval = 5 * time.Minute
		}
	} else if v := os.Getenv("RETRY_INTERVAL_MIN"); v != "" {
		min, err := strconv.Atoi(v)
		if err != nil {
			logPrint("ERROR", fmt.Sprintf("Invalid RETRY_INTERVAL_MIN: %v", err))
			retryInterval = 5 * time.Minute
		} else {
			retryInterval = time.Duration(min) * time.Minute
		}
	} else {
		retryInterval = 5 * time.Minute
	}

	// Backup timeout (default 10m)
	backupTimeout := 10 * time.Minute
	if v := os.Getenv("BACKUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			backupTimeout = d
		} else {
			logPrint("ERROR", fmt.Sprintf("Invalid BACKUP_TIMEOUT: %v", err))
		}
	}

	// Keep raw files (default false)
	keepRawFiles := false
	if v := os.Getenv("KEEP_RAW_FILES"); v != "" {
		if v == "1" || v == "true" || v == "TRUE" {
			keepRawFiles = true
		}
	}

	AppConfig = Config{
		MongoURI:      os.Getenv("MONGO_URI"),
		BackupPath:    os.Getenv("BACKUP_PATH"),
		MongodumpPath: os.Getenv("MONGODUMP_PATH"),
		Compression:   os.Getenv("COMPRESSION"),
		RetryInterval: retryInterval,
		MaxRetries:    atoiDefault(os.Getenv("MAX_RETRIES"), 5),
		MaxRetryDays:  atoiDefault(os.Getenv("MAX_RETRY_DAYS"), 7),
		BackupTimeout: backupTimeout,
		KeepRawFiles:  keepRawFiles,
	}

	// Validate config
	if AppConfig.MongoURI == "" {
		logPrint("ERROR", "MONGO_URI is required in .env")
	}
	if AppConfig.BackupPath == "" {
		logPrint("ERROR", "BACKUP_PATH is required in .env")
	}
	if AppConfig.MongodumpPath == "" {
		AppConfig.MongodumpPath = "mongodump" // fallback to default
	}
	logPrint("INFO", fmt.Sprintf("Config loaded: %+v", AppConfig))
}

func atoiDefault(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}

func logPrint(level, msg string) {
	fmt.Printf("[%s] %s %s\n", level, time.Now().Format("2006-01-02 15:04:05"), msg)
}
