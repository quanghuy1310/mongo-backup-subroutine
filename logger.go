package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

var (
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
)

var logFile *os.File

const maxLogSize = 500 * 1024 * 1024 // 500 MB

// InitLogger initializes loggers with file rotation
func InitLogger(logPath string) error {
	if logPath == "" {
		logPath = "./backup.log"
	}

	// Ensure directory exists
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Open log file (append mode)
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	logFile = f

	// MultiWriter: stdout + file
	mwOut := io.MultiWriter(os.Stdout, f)
	mwErr := io.MultiWriter(os.Stderr, f)

	Info = log.New(mwOut, "[INFO] ", log.LstdFlags)
	Warn = log.New(mwOut, "[WARN] ", log.LstdFlags)
	Error = log.New(mwErr, "[ERROR] ", log.LstdFlags)

	// Background rotation: daily + size
	go monitorLogFile(logPath)

	return nil
}

// monitorLogFile checks daily and size-based rotation
func monitorLogFile(basePath string) {
	for {
		time.Sleep(1 * time.Minute) // check every minute

		// Rotate by size
		if fi, err := os.Stat(basePath); err == nil {
			if fi.Size() >= maxLogSize {
				rotateLogFile(basePath)
			}
		}

		// Rotate by date
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		timeUntilMidnight := time.Until(next)
		if timeUntilMidnight < time.Minute { // near midnight
			rotateLogFile(basePath)
		}
	}
}

// rotateLogFile renames the current log file and opens a new one
func rotateLogFile(basePath string) {
	if logFile != nil {
		logFile.Close()
	}

	timestamp := time.Now().Format("2006_01_02_15_04_05")
	newName := basePath + "." + timestamp

	// Rename current log file
	os.Rename(basePath, newName)

	// Open new log file
	f, err := os.OpenFile(basePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[ERROR] Failed to create new log file: %v", err)
		return
	}
	logFile = f

	// Reset loggers
	mwOut := io.MultiWriter(os.Stdout, f)
	mwErr := io.MultiWriter(os.Stderr, f)
	Info.SetOutput(mwOut)
	Warn.SetOutput(mwOut)
	Error.SetOutput(mwErr)

	Info.Printf("Log rotated: %s -> %s", basePath, newName)
}

// CloseLogger closes the log file before exit
func CloseLogger() {
	if logFile != nil {
		logFile.Close()
	}
}
