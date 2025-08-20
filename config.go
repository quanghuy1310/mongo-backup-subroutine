package main

import (
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
	LogFile       string // Path to log file
}

var AppConfig Config

func LoadConfig() {
	if err := godotenv.Load(); err != nil {
		Warn.Println("Can't load .env file, using environment variables")
	}

	// Retry interval
	retryInterval := 5 * time.Minute
	if v := os.Getenv("RETRY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			retryInterval = d
		} else {
			Error.Printf("Invalid RETRY_INTERVAL: %v", err)
		}
	} else if v := os.Getenv("RETRY_INTERVAL_MIN"); v != "" {
		if min, err := strconv.Atoi(v); err == nil && min > 0 {
			retryInterval = time.Duration(min) * time.Minute
		} else {
			Error.Printf("Invalid RETRY_INTERVAL_MIN: %v", err)
		}
	}

	// Backup timeout
	backupTimeout := 10 * time.Minute
	if v := os.Getenv("BACKUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			backupTimeout = d
		} else {
			Error.Printf("Invalid BACKUP_TIMEOUT: %v", err)
		}
	}

	// Keep raw files
	keepRawFiles := false
	if v := os.Getenv("KEEP_RAW_FILES"); v == "1" || v == "true" || v == "TRUE" {
		keepRawFiles = true
	}

	// Worker count
	workerCount := runtime.NumCPU()
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		if val, err := strconv.Atoi(v); err == nil && val > 0 {
			workerCount = val
		} else {
			Warn.Printf("Invalid WORKER_COUNT: %s, fallback to %d", v, workerCount)
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
		LogFile:       os.Getenv("LOG_FILE"),
	}

	// Validate
	if AppConfig.MongoURI == "" {
		Error.Println("MONGO_URI is required in .env")
		os.Exit(1)
	}
	if AppConfig.BackupPath == "" {
		Error.Println("BACKUP_PATH is required in .env")
		os.Exit(1)
	}
	if AppConfig.MongodumpPath == "" {
		Info.Println("MONGODUMP_PATH not set, using default 'mongodump'")
		AppConfig.MongodumpPath = "mongodump"
	}

	Info.Printf("Config loaded: %+v", AppConfig)
}

func atoiDefault(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}
