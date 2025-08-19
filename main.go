package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	logPrint("INFO", "Mongo Backup Subroutine v1.0 starting...")
	LoadConfig()

	err := ConnectMongo(AppConfig.MongoURI)
	if err != nil {
		logPrint("ERROR", fmt.Sprintf("Failed to connect MongoDB: %v", err))
		return
	}

	yesterday := time.Now().AddDate(0, 0, -1)
	dbs, err := ListProviderDatabases()
	if err != nil {
		logPrint("ERROR", fmt.Sprintf("Failed to list databases: %v", err))
		return
	}

	logPrint("INFO", fmt.Sprintf("Found %d databases to backup.", len(dbs)))

	type BackupResult struct {
		DBName string
		Status string
		Error  error
	}

	workerCount := 5 // You can adjust this for optimal concurrency
	jobs := make(chan string, len(dbs))
	results := make(chan BackupResult, len(dbs))

	var wg sync.WaitGroup
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for dbName := range jobs {
				logPrint("INFO", fmt.Sprintf("Starting backup for DB: %s", dbName))
				err := BackupWithRetry(dbName, yesterday)
				status := "success"
				if err != nil {
					status = "failed"
				}
				results <- BackupResult{DBName: dbName, Status: status, Error: err}
			}
		}()
	}

	for _, db := range dbs {
		jobs <- db
	}
	close(jobs)
	wg.Wait()
	close(results)

	// Summarize results
	successCount := 0
	failCount := 0
	fmt.Println("\nBackup summary:")
	for res := range results {
		fmt.Printf("- %s: %s\n", res.DBName, res.Status)
		if res.Status == "success" {
			successCount++
		} else {
			failCount++
		}
	}
	fmt.Printf("\nTotal: %d, Success: %d, Failed: %d\n", len(dbs), successCount, failCount)
	logPrint("INFO", "All backups finished.")
}
