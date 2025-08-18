package main

import (
	"fmt"
	"os"
	"time"
)

func FormatDate(t time.Time) string {
	return t.Format("2006_01_02")
}

func BackupDir(dbName string, date time.Time) string {
	dir := fmt.Sprintf("%s/%s.%s", AppConfig.BackupPath, dbName, FormatDate(date))
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create backup directory: %v\n", err)
		return ""
	}
	return dir
}
