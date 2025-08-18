package main

import (
	"fmt"
	"os/exec"
	"time"
)

func BackupDatabase(dbName string, date time.Time) error {
	dir := BackupDir(dbName, date)

	done, err := IsBackupDone(dbName, FormatDate(date))
	if err != nil {
		return err
	}
	if done {
		fmt.Println("Backup already exists:", dbName, date)
		return nil
	}

	collection := fmt.Sprintf("GPS_%s", FormatDate(date))
	cmd := exec.Command("mongodump",
		"--uri", AppConfig.MongoURI,
		"--db", dbName,
		"--collection", collection,
		"--out", dir,
	)

	fmt.Println("Running mongodump:", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		SaveBackupStatus(dbName, FormatDate(date), "failed", string(output))
		return err
	}

	SaveBackupStatus(dbName, FormatDate(date), "success", "OK")
	fmt.Println("Backup successful:", dbName, collection)
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
