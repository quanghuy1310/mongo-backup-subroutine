package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	// bson is not needed in this file
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
	FileSizeMB int64
	Status     BackupStatus
	SkipReason string
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
		reason := "already exists"
		Info.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", dbName, result.Collection, reason)
		result.Status = StatusSkipped
		result.SkipReason = reason
		result.Error = fmt.Errorf("skipped:%s", reason)
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
		if strings.Contains(outStr, "ns not found") ||
			strings.Contains(outStr, fmt.Sprintf("collection '%s' does not exist", result.Collection)) {
			reason := "collection not found"
			Info.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", dbName, result.Collection, reason)
			result.Status = StatusSkipped
			result.SkipReason = reason
			SaveBackupStatus(dbName, result.Collection, string(StatusSkipped), reason)
			result.Error = fmt.Errorf("skipped:%s", reason)
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

	// Ensure dump actually produced files
	if _, err := os.Stat(bsonFile); os.IsNotExist(err) {
		reason := "no data dumped"
		Info.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", dbName, result.Collection, reason)
		result.Status = StatusSkipped
		result.SkipReason = reason
		SaveBackupStatus(dbName, result.Collection, string(StatusSkipped), reason)
		result.Error = fmt.Errorf("skipped:%s", reason)
		return result
	}

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
		// CHANGED: convert to MB
		result.FileSizeMB = info.Size() / (1024 * 1024)
	}
	result.BsonFile = s2BsonFile
	result.MetaFile = s2MetaFile
	result.Status = StatusSuccess
	result.Error = nil

	// Save metadata
	if metaErr := SaveBackupHistory(dbName, result.Collection, s2BsonFile, s2MetaFile, result.FileSizeMB, string(StatusSuccess), "s2", "OK"); metaErr != nil {
		Error.Printf("Failed to save backup metadata: %v", metaErr)
	}

	SaveBackupStatus(dbName, result.Collection, string(StatusSuccess), "OK")
	Info.Printf("Backup success: DB=%s Collection=%s File=%s Size=%d", dbName, result.Collection, s2BsonFile, result.FileSizeMB)

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
		if res.Error == nil || strings.HasPrefix(res.Error.Error(), "skipped:") {
			return attempt, res.Error
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
	if err != nil && strings.HasPrefix(err.Error(), "skipped:") {
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
		workerCount = runtime.NumCPU()
		Info.Printf("Worker count not configured or invalid, using default: %d workers", workerCount)
	} else {
		Info.Printf("Configured worker count: %d", workerCount)
	}

	type backupJob struct {
		DBName string
		Date   time.Time
	}

	type backupResult struct {
		DBName     string
		Date       time.Time
		Status     string
		Error      error
		Retries    int
		SkipReason string
	}

	// total jobs = len(dbs) * BackupDaysInterval
	totalJobs := len(dbs) * AppConfig.BackupDaysInterval
	jobs := make(chan backupJob, totalJobs)
	results := make(chan backupResult, totalJobs)
	var wg sync.WaitGroup

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Protect goroutine from panics to avoid crashing worker pool
			defer func() {
				if r := recover(); r != nil {
					Error.Printf("panic in backup worker: %v", r)
				}
			}()
			for job := range jobs {
				attempts, err := BackupWithRetry(job.DBName, job.Date)
				status := "success"
				skipReason := ""
				if err != nil {
					if strings.HasPrefix(err.Error(), "skipped:") {
						status = "skipped"
						skipReason = strings.TrimPrefix(err.Error(), "skipped:")
					} else {
						status = "failed"
					}
				}
				results <- backupResult{
					DBName:     job.DBName,
					Date:       job.Date,
					Status:     status,
					Error:      err,
					Retries:    attempts,
					SkipReason: skipReason,
				}
			}
		}()
	}

	// enqueue jobs for the last N days (including backupDate)
	for dayOffset := 0; dayOffset < AppConfig.BackupDaysInterval; dayOffset++ {
		d := backupDate.AddDate(0, 0, -dayOffset)
		for _, db := range dbs {
			jobs <- backupJob{DBName: db, Date: d}
		}
	}
	close(jobs)
	wg.Wait()
	close(results)

	for res := range results {
		switch res.Status {
		case "success":
			Info.Printf("[SUCCESS] DB=%s Date=%s (retries=%d)", res.DBName, FormatDate(res.Date), res.Retries)
		case "skipped":
			Warn.Printf("[SKIPPED] DB=%s Date=%s (%s)", res.DBName, FormatDate(res.Date), res.SkipReason)
		case "failed":
			Error.Printf("[FAILED] DB=%s Date=%s (retries=%d, error=%v)", res.DBName, FormatDate(res.Date), res.Retries, res.Error)
		default:
			Warn.Printf("[UNKNOWN STATUS] DB=%s Date=%s: %s", res.DBName, FormatDate(res.Date), res.Status)
		}
	}
}

// RunFullBackupSingleDay runs backups for all provider DBs for a single date.
// This is used by CLI --all to keep CLI behavior predictable.
func RunFullBackupSingleDay(backupDate time.Time) {
	dbs, err := ListProviderDatabases()
	if err != nil {
		Error.Printf("Failed to list databases: %v", err)
		return
	}
	if len(dbs) == 0 {
		Info.Println("No databases found for backup.")
		return
	}

	Info.Printf("Starting single-day backup for %d databases (date=%s)", len(dbs), FormatDate(backupDate))
	workerCount := AppConfig.WorkerCount
	if workerCount <= 0 {
		workerCount = runtime.NumCPU()
		Info.Printf("Worker count not configured or invalid, using default: %d workers", workerCount)
	}

	type backupJob struct {
		DBName string
		Date   time.Time
	}

	type backupResult struct {
		DBName     string
		Date       time.Time
		Status     string
		Error      error
		Retries    int
		SkipReason string
	}

	totalJobs := len(dbs)
	jobs := make(chan backupJob, totalJobs)
	results := make(chan backupResult, totalJobs)
	var wg sync.WaitGroup

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					Error.Printf("panic in backup worker: %v", r)
				}
			}()
			for job := range jobs {
				attempts, err := BackupWithRetry(job.DBName, job.Date)
				status := "success"
				skipReason := ""
				if err != nil {
					if strings.HasPrefix(err.Error(), "skipped:") {
						status = "skipped"
						skipReason = strings.TrimPrefix(err.Error(), "skipped:")
					} else {
						status = "failed"
					}
				}
				results <- backupResult{
					DBName:     job.DBName,
					Date:       job.Date,
					Status:     status,
					Error:      err,
					Retries:    attempts,
					SkipReason: skipReason,
				}
			}
		}()
	}

	for _, db := range dbs {
		jobs <- backupJob{DBName: db, Date: backupDate}
	}
	close(jobs)
	wg.Wait()
	close(results)

	for res := range results {
		switch res.Status {
		case "success":
			Info.Printf("[SUCCESS] DB=%s Date=%s (retries=%d)", res.DBName, FormatDate(res.Date), res.Retries)
		case "skipped":
			Warn.Printf("[SKIPPED] DB=%s Date=%s (%s)", res.DBName, FormatDate(res.Date), res.SkipReason)
		case "failed":
			Error.Printf("[FAILED] DB=%s Date=%s (retries=%d, error=%v)", res.DBName, FormatDate(res.Date), res.Retries, res.Error)
		default:
			Warn.Printf("[UNKNOWN STATUS] DB=%s Date=%s: %s", res.DBName, FormatDate(res.Date), res.Status)
		}
	}
}
