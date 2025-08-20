package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/s2"
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

// BackupDatabase performs one backup attempt
func BackupDatabase(dbName string, date time.Time) BackupResult {
	result := BackupResult{
		Database:   dbName,
		Collection: fmt.Sprintf("GPS_%s", FormatDate(date)),
		Status:     "failed",
	}

	dir := BackupDir(dbName, date)
	log.Printf("[DEBUG] Starting backup for DB=%s, date=%s", dbName, FormatDate(date))

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

	// run mongodump with context timeout
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

	// compress BSON
	if err := CompressFileS2(bsonFile, s2BsonFile); err != nil {
		log.Printf("[ERROR] Failed to compress BSON: %v", err)
		result.Error = err
		return result
	}

	// compress metadata
	if err := CompressFileS2(metaFile, s2MetaFile); err != nil {
		log.Printf("[WARN] Failed to compress metadata: %v", err)
	}

	// get file size of bson.s2
	info, err := os.Stat(s2BsonFile)
	if err == nil {
		result.FileSize = info.Size()
	}

	result.BsonFile = s2BsonFile
	result.MetaFile = s2MetaFile
	result.Status = "success"
	result.Error = nil

	// save metadata to MongoDB
	metaErr := SaveBackupHistory(dbName, result.Collection, s2BsonFile, s2MetaFile, result.FileSize, "success", "s2", "OK")
	if metaErr != nil {
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

// Compress BSON/JSON file to s2 format
func CompressFileS2(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()

	writer := s2.NewWriter(out)
	defer writer.Close()

	buf := make([]byte, 1<<20) // 1MB buffer
	_, err = io.CopyBuffer(writer, in, buf)
	return err
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
