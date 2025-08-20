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
	LogFile       string
	ScheduleHour  int
	ScheduleMin   int
}

var AppConfig Config

func LoadConfig() {
	if err := godotenv.Load(); err != nil {
		Warn.Println("Can't load .env, using environment variables")
	}

	retryInterval := 5 * time.Minute
	if v := os.Getenv("RETRY_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			retryInterval = d
		}
	}

	backupTimeout := 10 * time.Minute
	if v := os.Getenv("BACKUP_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			backupTimeout = d
		}
	}

	keepRawFiles := false
	if v := os.Getenv("KEEP_RAW_FILES"); v == "1" || v == "true" || v == "TRUE" {
		keepRawFiles = true
	}

	workerCount := runtime.NumCPU()
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		if val, err := strconv.Atoi(v); err == nil && val > 0 {
			workerCount = val
		}
	}

	hour := 2
	minute := 0
	if v := os.Getenv("SCHEDULE_HOUR"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			hour = val
		}
	}
	if v := os.Getenv("SCHEDULE_MINUTE"); v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			minute = val
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
		ScheduleHour:  hour,
		ScheduleMin:   minute,
	}

	if AppConfig.MongoURI == "" || AppConfig.BackupPath == "" {
		Error.Println("MONGO_URI and BACKUP_PATH are required")
		os.Exit(1)
	}
}

func atoiDefault(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}
