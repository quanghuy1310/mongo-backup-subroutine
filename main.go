package main

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

func main() {
	logPrint("INFO", "Mongo Backup Subroutine v2.0 starting...")
	LoadConfig()

	// Kết nối MongoDB
	err := ConnectMongo(AppConfig.MongoURI)
	if err != nil {
		logPrint("ERROR", fmt.Sprintf("Failed to connect MongoDB: %v", err))
		os.Exit(1)
	}
	defer DisconnectMongo()

	// Ngày backup: mặc định là hôm qua
	backupDate := time.Now().AddDate(0, 0, -1)

	// Lấy danh sách database cần backup
	dbs, err := ListProviderDatabases()
	if err != nil {
		logPrint("ERROR", fmt.Sprintf("Failed to list databases: %v", err))
		os.Exit(1)
	}
	if len(dbs) == 0 {
		logPrint("INFO", "No databases found for backup. Exiting.")
		return
	}

	logPrint("INFO", fmt.Sprintf("Found %d databases to backup.", len(dbs)))
	logPrint("INFO", fmt.Sprintf("Worker count: %d", AppConfig.WorkerCount))

	// Kết quả backup từng database
	type BackupResult struct {
		DBName  string
		Status  string // "success", "failed", "skipped"
		Error   error
		Retries int
	}

	// Worker setup
	workerCount := AppConfig.WorkerCount
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() * 2
		logPrint("WARN", fmt.Sprintf("Invalid WORKER_COUNT, fallback to %d workers", workerCount))
	}

	jobs := make(chan string, len(dbs))
	results := make(chan BackupResult, len(dbs))

	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for dbName := range jobs {
				logPrint("INFO", fmt.Sprintf("[Worker %d] Starting backup for DB: %s", workerID, dbName))

				// Retry + backup
				retries, err := BackupWithRetry(dbName, backupDate)
				status := "success"
				if err != nil {
					if err.Error() == "skipped" { // BackupDatabase trả về error = "skipped" nếu collection không tồn tại
						status = "skipped"
						logPrint("WARN", fmt.Sprintf("[Worker %d] Backup skipped for DB %s (collection not found)", workerID, dbName))
					} else {
						status = "failed"
						logPrint("ERROR", fmt.Sprintf("[Worker %d] Backup failed for DB %s after %d retries: %v", workerID, dbName, retries, err))
					}
				} else {
					logPrint("INFO", fmt.Sprintf("[Worker %d] Backup completed for DB %s (retries: %d)", workerID, dbName, retries))
				}

				results <- BackupResult{
					DBName:  dbName,
					Status:  status,
					Error:   err,
					Retries: retries,
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
			fmt.Printf("- %s: ⚠️ skipped (collection not found)\n", res.DBName)
			skippedCount++
		case "failed":
			fmt.Printf("- %s: ❌ failed (after %d retries, error: %v)\n", res.DBName, res.Retries, res.Error)
			failCount++
		}
	}

	fmt.Printf("\nTotal: %d, Success: %d, Skipped: %d, Failed: %d\n", len(dbs), successCount, skippedCount, failCount)
	logPrint("INFO", "All backups finished.")
}
