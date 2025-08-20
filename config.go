package main

import (
	"fmt"
	"os"
	"runtime"
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
	WorkerCount   int
}

var AppConfig Config

func LoadConfig() {
	err := godotenv.Load()
	if err != nil {
		logPrint("WARN", "Can't load .env file, using environment variables")
	}

	// Retry interval (support REATTEMPT_INTERVAL or REATTEMPT_INTERVAL_MIN)
	retryInterval := 5 * time.Minute // default
	if v := os.Getenv("RETRY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			retryInterval = d
		} else {
			logPrint("ERROR", fmt.Sprintf("Invalid RETRY_INTERVAL: %v", err))
		}
	} else if v := os.Getenv("RETRY_INTERVAL_MIN"); v != "" {
		if min, err := strconv.Atoi(v); err == nil && min > 0 {
			retryInterval = time.Duration(min) * time.Minute
		} else {
			logPrint("ERROR", fmt.Sprintf("Invalid RETRY_INTERVAL_MIN: %v", err))
		}
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
	if v := os.Getenv("KEEP_RAW_FILES"); v == "1" || v == "true" || v == "TRUE" {
		keepRawFiles = true
	}

	// WorkerCount (default: runtime.NumCPU())
	workerCount := runtime.NumCPU()
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		if val, err := strconv.Atoi(v); err == nil && val > 0 {
			workerCount = val
		} else {
			logPrint("WARN", fmt.Sprintf("Invalid WORKER_COUNT: %s, fallback to %d", v, workerCount))
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
		WorkerCount:   workerCount,
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
