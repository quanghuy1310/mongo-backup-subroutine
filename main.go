package main

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

func main() {
	InitLogger(AppConfig.LogFile)
	LoadConfig()
	Info.Println("Mongo Backup Subroutine v2.0 starting...")

	// Kết nối MongoDB
	err := ConnectMongo(AppConfig.MongoURI)
	if err != nil {
		Error.Printf("Failed to connect MongoDB: %v", err)
		os.Exit(1)
	}
	defer DisconnectMongo()

	// Ngày backup: mặc định là hôm qua
	backupDate := time.Now().AddDate(0, 0, -1)

	// Lấy danh sách database cần backup
	dbs, err := ListProviderDatabases()
	if err != nil {
		Error.Printf("Failed to list databases: %v", err)
		os.Exit(1)
	}
	if len(dbs) == 0 {
		Info.Println("No databases found for backup. Exiting.")
		return
	}

	Info.Printf("Found %d databases to backup.", len(dbs))
	Info.Printf("Worker count: %d", AppConfig.WorkerCount)

	type BackupResult struct {
		DBName     string
		Status     string // "success", "failed", "skipped"
		Error      error
		Retries    int
		SkipReason string
	}

	// Setup workers
	workerCount := AppConfig.WorkerCount
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() * 2
		Info.Printf("Invalid WORKER_COUNT, fallback to %d workers", workerCount)
	}

	jobs := make(chan string, len(dbs))
	results := make(chan BackupResult, len(dbs))

	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for dbName := range jobs {
				Info.Printf("[Worker %d] Starting backup for DB: %s", workerID, dbName)

				attempts, err := BackupWithRetry(dbName, backupDate)
				status := "success"
				skipReason := ""

				if err != nil {
					if err.Error() == "skipped" {
						status = "skipped"
						skipReason = "collection not found"
						Info.Printf("[Worker %d] Backup skipped for DB %s (%s)", workerID, dbName, skipReason)
					} else {
						status = "failed"
						Error.Printf("[Worker %d] Backup failed for DB %s after %d retries: %v", workerID, dbName, attempts, err)
					}
				} else {
					Info.Printf("[Worker %d] Backup completed for DB %s (retries: %d)", workerID, dbName, attempts)
				}

				results <- BackupResult{
					DBName:     dbName,
					Status:     status,
					Error:      err,
					Retries:    attempts,
					SkipReason: skipReason,
				}
			}
		}(w + 1)
	}

	// Push jobs
	for _, db := range dbs {
		jobs <- db
	}
	close(jobs)

	// Wait all workers
	wg.Wait()
	close(results)

	// Summary
	successCount := 0
	failCount := 0
	skippedCount := 0

	fmt.Println("\nBackup Summary:")
	for res := range results {
		switch res.Status {
		case "success":
			fmt.Printf("- %s: ✅ success (retries: %d)\n", res.DBName, res.Retries)
			successCount++
		case "skipped":
			fmt.Printf("- %s: ⚠️ skipped (%s)\n", res.DBName, res.SkipReason)
			skippedCount++
		case "failed":
			fmt.Printf("- %s: ❌ failed (after %d retries, error: %v)\n", res.DBName, res.Retries, res.Error)
			failCount++
		}
	}

	Info.Printf("Backup Summary: Total=%d, Success=%d, Skipped=%d, Failed=%d", len(dbs), successCount, skippedCount, failCount)
	Info.Println("All backups finished.")
}
