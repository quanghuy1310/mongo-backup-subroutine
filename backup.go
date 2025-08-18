package main

import (
	"fmt"
	"os/exec"
	"time"
)

func BackupDatabase(dbName string, date time.Time) error {
	dir := BackupDir(dbName, date)

	fmt.Printf("[DEBUG] Starting backup for DB: %s, date: %s\n", dbName, FormatDate(date))

	done, err := IsBackupDone(dbName, FormatDate(date))
	if err != nil {
		fmt.Printf("[ERROR] Failed to check backup status: %v\n", err)
		return err
	}
	if done {
		fmt.Printf("[INFO] Backup already exists: %s, date: %s\n", dbName, FormatDate(date))
		return nil
	}

	collection := fmt.Sprintf("GPS_%s", FormatDate(date))
	cmd := exec.Command("mongodump",
		"--uri", AppConfig.MongoURI,
		"--db", dbName,
		"--collection", collection,
		"--out", dir,
	)

	fmt.Println("[INFO] Running mongodump:", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("[ERROR] mongodump failed: %v\nOutput: %s\n", err, string(output))
		SaveBackupStatus(dbName, FormatDate(date), "failed", string(output))
		return err
	}

	SaveBackupStatus(dbName, FormatDate(date), "success", "OK")
	fmt.Printf("[INFO] Backup successful: %s, collection: %s\n", dbName, collection)
	return nil
}

func BackupWithRetry(dbName string, date time.Time) {
	for i := 0; i < AppConfig.MaxRetries; i++ {
		err := BackupDatabase(dbName, date)
		if err == nil {
			return
		}
		fmt.Println("Backup failed, retrying after:", AppConfig.RetryInterval)
		time.Sleep(AppConfig.RetryInterval)
	}
	fmt.Println("Backup failed after max retries:", dbName, date)
}
