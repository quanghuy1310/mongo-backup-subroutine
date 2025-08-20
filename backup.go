package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
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

// BackupDatabase performs one backup attempt for a single DB & date
func BackupDatabase(dbName string, date time.Time) BackupResult {
	result := BackupResult{
		Database:   dbName,
		Collection: fmt.Sprintf("GPS_%s", FormatDate(date)),
		Status:     "failed",
	}

	dir := BackupDir(dbName, date)
	log.Printf("[DEBUG] Starting backup for DB=%s, date=%s", dbName, FormatDate(date))
	log.Printf("[INFO] Worker count: %d", AppConfig.WorkerCount) // log worker count

	// check if already backed up
	done, err := IsBackupDone(dbName, FormatDate(date))
	if err != nil {
		log.Printf("[ERROR] Failed to check backup status: %v", err)
		result.Error = err
		return result
	}
	if done {
		log.Printf("[INFO] Backup already exists: %s, date: %s", dbName, FormatDate(date))
		result.Status = "exists"
		return result
	}

	// run mongodump with timeout
	ctx, cancel := context.WithTimeout(context.Background(), AppConfig.BackupTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, AppConfig.MongodumpPath,
		"--uri", AppConfig.MongoURI,
		"--db", dbName,
		"--collection", result.Collection,
		"--out", dir,
	)
	log.Printf("[INFO] Running mongodump: %s", cmd.String())
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		log.Printf("[ERROR] mongodump timed out")
		SaveBackupStatus(dbName, FormatDate(date), "failed", "timeout")
		result.Error = ctx.Err()
		return result
	}

	if err != nil {
		// check if collection does not exist
		if string(output) != "" && (string(output) == fmt.Sprintf("collection '%s' does not exist", result.Collection) ||
			string(output) == "ns not found") {
			log.Printf("[WARN] Collection %s does not exist in DB %s, skipping backup", result.Collection, dbName)
			result.Status = "skipped"
			SaveBackupStatus(dbName, FormatDate(date), "skipped", "collection not found")
			return result
		}
		log.Printf("[ERROR] mongodump failed: %v\nOutput: %s", err, string(output))
		SaveBackupStatus(dbName, FormatDate(date), "failed", string(output))
		result.Error = err
		return result
	}

	// file paths
	bsonFile := filepath.Join(dir, dbName, result.Collection+".bson")
	metaFile := filepath.Join(dir, dbName, result.Collection+".metadata.json")
	s2BsonFile := bsonFile + ".s2"
	s2MetaFile := metaFile + ".s2"

	// check BSON file exists
	if _, err := os.Stat(bsonFile); os.IsNotExist(err) {
		log.Printf("[WARN] BSON file not found for DB=%s, collection=%s, skipping", dbName, result.Collection)
		result.Status = "skipped"
		SaveBackupStatus(dbName, FormatDate(date), "skipped", "BSON not found")
		return result
	}

	// compress BSON and metadata
	filesToCompress := map[string]string{
		bsonFile: s2BsonFile,
		metaFile: s2MetaFile,
	}
	if err := CompressFilesS2(filesToCompress); err != nil {
		log.Printf("[ERROR] Failed to compress files: %v", err)
		result.Error = err
		return result
	}

	if info, err := os.Stat(s2BsonFile); err == nil {
		result.FileSize = info.Size()
	}
	result.BsonFile = s2BsonFile
	result.MetaFile = s2MetaFile
	result.Status = "success"
	result.Error = nil

	// save metadata
	if metaErr := SaveBackupHistory(dbName, result.Collection, s2BsonFile, s2MetaFile, result.FileSize, "success", "s2", "OK"); metaErr != nil {
		log.Printf("[ERROR] Failed to save backup metadata: %v", metaErr)
	}

	SaveBackupStatus(dbName, FormatDate(date), "success", "OK")
	log.Printf("[INFO] Backup successful: %s, collection=%s", dbName, result.Collection)

	// cleanup raw files if configured
	if !AppConfig.KeepRawFiles {
		os.Remove(bsonFile)
		os.Remove(metaFile)
	}

	return result
}

// Save backup metadata to MongoDB
func SaveBackupHistory(dbName, collection, bsonFile, metaFile string, fileSize int64, status, compression, msg string) error {
	coll := mongoClient.Database("admin").Collection("backupHistory")
	_, err := coll.InsertOne(context.Background(), map[string]interface{}{
		"database":    dbName,
		"collection":  collection,
		"bsonFile":    bsonFile,
		"metaFile":    metaFile,
		"fileSize":    fileSize,
		"status":      status,
		"compression": compression,
		"message":     msg,
		"timestamp":   time.Now(),
	})
	return err
}

// Retry wrapper: return error and actual attempts
func BackupWithRetry(dbName string, date time.Time) (int, error) {
	var lastErr error
	var attempt int
	for i := 0; i < AppConfig.MaxRetries; i++ {
		attempt = i + 1
		res := BackupDatabase(dbName, date)
		if res.Error == nil {
			return attempt, nil
		}
		if !isRecoverableError(res.Error) {
			log.Printf("[ERROR] Non-recoverable error: %v", res.Error)
			return attempt, res.Error
		}
		lastErr = res.Error
		log.Printf("[WARN] Backup failed, retrying after %s (attempt %d/%d)", AppConfig.RetryInterval, attempt, AppConfig.MaxRetries)
		time.Sleep(AppConfig.RetryInterval)
	}
	log.Printf("[ERROR] Backup failed after max retries: %s %s", dbName, FormatDate(date))
	return attempt, lastErr
}

// Simple heuristic: retry only if error is temporary
func isRecoverableError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	// add more recoverable conditions if needed
	return true
}
