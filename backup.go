package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// BackupStatus defines enum for backup result
type BackupStatus string

const (
	StatusSuccess BackupStatus = "success"
	StatusFailed  BackupStatus = "failed"
	StatusSkipped BackupStatus = "skipped"
)

// BackupResult stores the result of a backup
type BackupResult struct {
	Database   string
	Collection string
	BsonFile   string
	MetaFile   string
	FileSize   int64
	Status     BackupStatus
	Error      error
}

// BackupDatabase performs one backup attempt for a single DB & collection/date
func BackupDatabase(dbName string, date time.Time) BackupResult {
	result := BackupResult{
		Database:   dbName,
		Collection: fmt.Sprintf("GPS_%s", FormatDate(date)),
		Status:     StatusFailed,
	}

	dir, err := BackupDir(dbName, date)
	if err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=%v", dbName, result.Collection, err)
		result.Error = err
		return result
	}

	Info.Printf("Start backup: DB=%s Collection=%s", dbName, result.Collection)

	// Check if already backed up
	done, err := IsBackupDone(dbName, result.Collection)
	if err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=%v", dbName, result.Collection, err)
		result.Error = err
		return result
	}
	if done {
		Info.Printf("Backup skipped: DB=%s Collection=%s Reason=already exists", dbName, result.Collection)
		result.Status = StatusSkipped
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
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=timeout", dbName, result.Collection)
		SaveBackupStatus(dbName, result.Collection, string(StatusFailed), "timeout")
		result.Error = ctx.Err()
		return result
	}

	if err != nil {
		outStr := string(output)
		if strings.Contains(outStr, "ns not found") || strings.Contains(outStr, fmt.Sprintf("collection '%s' does not exist", result.Collection)) {
			Info.Printf("Backup skipped: DB=%s Collection=%s Reason=collection not found", dbName, result.Collection)
			result.Status = StatusSkipped
			SaveBackupStatus(dbName, result.Collection, string(StatusSkipped), "collection not found")
			result.Error = errors.New("skipped")
			return result
		}
		Error.Printf("Backup failed: DB=%s Collection=%s Error=%v Output=%s", dbName, result.Collection, err, outStr)
		SaveBackupStatus(dbName, result.Collection, string(StatusFailed), outStr)
		result.Error = fmt.Errorf("%v (output: %s)", err, outStr)
		return result
	}

	// Correct mongodump path: nested dbName folder
	bsonFile := filepath.Join(dir, dbName, result.Collection+".bson")
	metaFile := filepath.Join(dir, dbName, result.Collection+".metadata.json")
	s2BsonFile := bsonFile + ".s2"
	s2MetaFile := metaFile + ".s2"

	// Check BSON integrity
	if err := CheckBsonIntegrity(bsonFile); err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=BSON integrity check failed %v", dbName, result.Collection, err)
		SaveBackupStatus(dbName, result.Collection, string(StatusFailed), "BSON integrity failed")
		result.Error = err
		return result
	}

	// Check metadata.json validity
	if err := CheckMetadataIntegrity(metaFile); err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=metadata integrity check failed %v", dbName, result.Collection, err)
		SaveBackupStatus(dbName, result.Collection, string(StatusFailed), "metadata integrity failed")
		result.Error = err
		return result
	}

	// Compress files
	filesToCompress := map[string]string{
		bsonFile: s2BsonFile,
		metaFile: s2MetaFile,
	}
	if err := CompressFilesS2(filesToCompress); err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=compress error %v", dbName, result.Collection, err)
		result.Error = err
		SaveBackupStatus(dbName, result.Collection, string(StatusFailed), "compress error")
		return result
	}

	if info, err := os.Stat(s2BsonFile); err == nil {
		result.FileSize = info.Size()
	}
	result.BsonFile = s2BsonFile
	result.MetaFile = s2MetaFile
	result.Status = StatusSuccess
	result.Error = nil

	// Save metadata
	if metaErr := SaveBackupHistory(dbName, result.Collection, s2BsonFile, s2MetaFile, result.FileSize, string(StatusSuccess), "s2", "OK"); metaErr != nil {
		Error.Printf("Failed to save backup metadata: %v", metaErr)
	}

	SaveBackupStatus(dbName, result.Collection, string(StatusSuccess), "OK")
	Info.Printf("Backup success: DB=%s Collection=%s File=%s Size=%d", dbName, result.Collection, s2BsonFile, result.FileSize)

	// Cleanup raw files
	if !AppConfig.KeepRawFiles {
		os.Remove(bsonFile)
		os.Remove(metaFile)
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
			Error.Printf("Backup non-recoverable: DB=%s Collection=%s Error=%v", dbName, res.Collection, res.Error)
			return attempt, res.Error
		}
		lastErr = res.Error
		time.Sleep(AppConfig.RetryInterval)
	}
	Error.Printf("Backup failed after max retries: DB=%s Collection=%s Error=%v", dbName, FormatDate(date), lastErr)
	return attempt, lastErr
}

func isRecoverableError(err error) bool {
	if err == context.DeadlineExceeded {
		return true
	}
	if err != nil && err.Error() == "skipped" {
		return false
	}
	return true
}

func RunFullBackup(backupDate time.Time) {
	dbs, err := ListProviderDatabases()
	if err != nil {
		Error.Printf("Failed to list databases: %v", err)
		return
	}
	if len(dbs) == 0 {
		Info.Println("No databases found for backup.")
		return
	}

	Info.Printf("Starting backup for %d databases", len(dbs))
	workerCount := AppConfig.WorkerCount
	if workerCount <= 0 {
		workerCount = 2 * runtime.NumCPU()
	}

	type backupResult struct {
		DBName     string
		Status     string
		Error      error
		Retries    int
		SkipReason string
	}

	jobs := make(chan string, len(dbs))
	results := make(chan backupResult, len(dbs))
	var wg sync.WaitGroup

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dbName := range jobs {
				attempts, err := BackupWithRetry(dbName, backupDate)
				status := "success"
				skipReason := ""
				if err != nil {
					if err.Error() == "skipped" {
						status = "skipped"
						skipReason = "collection not found or empty"
					} else {
						status = "failed"
					}
				}
				results <- backupResult{
					DBName:     dbName,
					Status:     status,
					Error:      err,
					Retries:    attempts,
					SkipReason: skipReason,
				}
			}
		}()
	}

	for _, db := range dbs {
		jobs <- db
	}
	close(jobs)
	wg.Wait()
	close(results)

	for res := range results {
		switch res.Status {
		case "success":
			Info.Printf("[SUCCESS] DB=%s (retries=%d)", res.DBName, res.Retries)
		case "skipped":
			Warn.Printf("[SKIPPED] DB=%s (%s)", res.DBName, res.SkipReason)
		case "failed":
			Error.Printf("[FAILED] DB=%s (retries=%d, error=%v)", res.DBName, res.Retries, res.Error)
		default:
			Warn.Printf("[UNKNOWN STATUS] DB=%s: %s", res.DBName, res.Status)
		}
	}
}
