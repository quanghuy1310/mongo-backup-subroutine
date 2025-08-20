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

const maxLogSize = 500 * 1024 * 1024 // 500MB

func InitLogger(logPath string) error {
	if logPath == "" {
		logPath = "/var/log/mongo_backup.log"
	}
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	logFile = f

	mwOut := io.MultiWriter(os.Stdout, f)
	mwErr := io.MultiWriter(os.Stderr, f)

	Info = log.New(mwOut, "[INFO] ", log.LstdFlags)
	Warn = log.New(mwOut, "[WARN] ", log.LstdFlags)
	Error = log.New(mwErr, "[ERROR] ", log.LstdFlags)

	go monitorLogFile(logPath)
	return nil
}

func monitorLogFile(basePath string) {
	for {
		time.Sleep(1 * time.Minute)
		if fi, err := os.Stat(basePath); err == nil && fi.Size() >= maxLogSize {
			rotateLogFile(basePath)
		}
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
		if time.Until(next) < time.Minute {
			rotateLogFile(basePath)
		}
	}
}

func rotateLogFile(basePath string) {
	if logFile != nil {
		logFile.Close()
	}

	timestamp := time.Now().Format("2006_01_02_15_04_05")
	newName := basePath + "." + timestamp
	os.Rename(basePath, newName)

	f, err := os.OpenFile(basePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[ERROR] Failed to create new log file: %v", err)
		return
	}
	logFile = f
	mwOut := io.MultiWriter(os.Stdout, f)
	mwErr := io.MultiWriter(os.Stderr, f)
	Info.SetOutput(mwOut)
	Warn.SetOutput(mwOut)
	Error.SetOutput(mwErr)
	Info.Printf("Log rotated: %s -> %s", basePath, newName)
}

func CloseLogger() {
	if logFile != nil {
		logFile.Close()
	}
}
