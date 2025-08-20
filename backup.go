package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// BackupResult stores the result of a backup
type BackupResult struct {
	Database   string
	Collection string
	BsonFile   string
	MetaFile   string
	FileSize   int64
	Status     string
	Error      error
}

// BackupDatabase performs one backup attempt for a single DB & collection/date
func BackupDatabase(dbName string, date time.Time) BackupResult {
	result := BackupResult{
		Database:   dbName,
		Collection: fmt.Sprintf("GPS_%s", FormatDate(date)),
		Status:     "failed",
	}

	dir, err := BackupDir(dbName, date)
	if err != nil {
		Error.Printf("Failed to get backup dir: %v", err)
		result.Error = err
		return result
	}
	Info.Printf("Starting backup for DB=%s, Collection=%s", dbName, result.Collection)
	Info.Printf("Worker count: %d", AppConfig.WorkerCount)

	// Check if already backed up
	done, err := IsBackupDone(dbName, result.Collection)
	if err != nil {
		Error.Printf("Failed to check backup status: %v", err)
		result.Error = err
		return result
	}
	if done {
		Info.Printf("Backup already exists: DB=%s, Collection=%s", dbName, result.Collection)
		result.Status = "exists"
		return result
	}

	// Run mongodump with timeout
	ctx, cancel := context.WithTimeout(context.Background(), AppConfig.BackupTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, AppConfig.MongodumpPath,
		"--uri", AppConfig.MongoURI,
		"--db", dbName,
		"--collection", result.Collection,
		"--out", dir,
	)
	Info.Printf("Running mongodump: %s", cmd.String())
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		Error.Printf("mongodump timed out for DB=%s, Collection=%s", dbName, result.Collection)
		SaveBackupStatus(dbName, result.Collection, "failed", "timeout")
		result.Error = ctx.Err()
		return result
	}

	if err != nil {
		outStr := string(output)
		if strings.Contains(outStr, "ns not found") || strings.Contains(outStr, fmt.Sprintf("collection '%s' does not exist", result.Collection)) {
			Info.Printf("Collection %s does not exist in DB %s, skipping backup", result.Collection, dbName)
			result.Status = "skipped"
			SaveBackupStatus(dbName, result.Collection, "skipped", "collection not found")
			result.Error = errors.New("skipped") // notify main.go
			return result
		}
		Error.Printf("mongodump failed: %v\nOutput: %s", err, outStr)
		SaveBackupStatus(dbName, result.Collection, "failed", outStr)
		result.Error = fmt.Errorf("%v (output: %s)", err, outStr)
		return result
	}

	// File paths
	bsonFile := filepath.Join(dir, result.Collection+".bson")
	metaFile := filepath.Join(dir, result.Collection+".metadata.json")
	s2BsonFile := bsonFile + ".s2"
	s2MetaFile := metaFile + ".s2"

	// Check BSON exists
	if _, err := os.Stat(bsonFile); os.IsNotExist(err) {
		Info.Printf("BSON file not found for DB=%s, Collection=%s, skipping", dbName, result.Collection)
		result.Status = "skipped"
		SaveBackupStatus(dbName, result.Collection, "skipped", "BSON not found")
		result.Error = errors.New("skipped")
		return result
	}

	// Compress files
	filesToCompress := map[string]string{
		bsonFile: s2BsonFile,
		metaFile: s2MetaFile,
	}
	if err := CompressFilesS2(filesToCompress); err != nil {
		Error.Printf("Failed to compress files: %v", err)
		result.Error = err
		SaveBackupStatus(dbName, result.Collection, "failed", "compress error")
		return result
	}

	// Set result
	if info, err := os.Stat(s2BsonFile); err == nil {
		result.FileSize = info.Size()
	}
	result.BsonFile = s2BsonFile
	result.MetaFile = s2MetaFile
	result.Status = "success"
	result.Error = nil

	// Save metadata
	if metaErr := SaveBackupHistory(dbName, result.Collection, s2BsonFile, s2MetaFile, result.FileSize, "success", "s2", "OK"); metaErr != nil {
		Error.Printf("Failed to save backup metadata: %v", metaErr)
	}

	SaveBackupStatus(dbName, result.Collection, "success", "OK")
	Info.Printf("Backup successful: DB=%s, Collection=%s", dbName, result.Collection)

	// Cleanup raw files
	if !AppConfig.KeepRawFiles {
		if err := os.Remove(bsonFile); err != nil {
			Error.Printf("Failed to remove raw BSON: %v", err)
		}
		if err := os.Remove(metaFile); err != nil {
			Error.Printf("Failed to remove raw metadata: %v", err)
		}
	}

	return result
}

// Retry wrapper with intelligent logic
func BackupWithRetry(dbName string, date time.Time) (int, error) {
	var lastErr error
	var attempt int
	for i := 0; i < AppConfig.MaxRetries; i++ {
		attempt = i + 1
		res := BackupDatabase(dbName, date)
		if res.Error == nil || res.Error.Error() == "skipped" {
			return attempt, nil
		}
		if !isRecoverableError(res.Error) {
			Error.Printf("Non-recoverable error: %v", res.Error)
			return attempt, res.Error
		}
		lastErr = res.Error
		Info.Printf("Backup failed, retrying after %s (attempt %d/%d)", AppConfig.RetryInterval, attempt, AppConfig.MaxRetries)
		time.Sleep(AppConfig.RetryInterval)
	}
	Error.Printf("Backup failed after max retries: %s %s", dbName, FormatDate(date))
	return attempt, lastErr
}

// Simple heuristic: retry only temporary errors
func isRecoverableError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	if err != nil && err.Error() == "skipped" {
		return false
	}
	return true
}

// Backup multiple DBs concurrently
func BackupDatabasesConcurrently(dbs []string, date time.Time) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, AppConfig.WorkerCount) // concurrency limit

	for _, db := range dbs {
		wg.Add(1)
		sem <- struct{}{}
		go func(dbName string) {
			defer wg.Done()
			defer func() { <-sem }()
			attempts, err := BackupWithRetry(dbName, date)
			if err != nil {
				Error.Printf("Backup failed for DB=%s after %d attempts: %v", dbName, attempts, err)
			} else {
				Info.Printf("Backup succeeded for DB=%s in %d attempts", dbName, attempts)
			}
		}(db)
	}

	wg.Wait()
}
