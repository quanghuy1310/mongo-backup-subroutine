package main

import (
	"os"
	"testing"
	"time"
)

func TestFormatDate(t *testing.T) {
	d := time.Date(2025, 10, 21, 0, 0, 0, 0, time.UTC)
	if got := FormatDate(d); got != "2025_10_21" {
		t.Fatalf("FormatDate returned %s, want 2025_10_21", got)
	}
}

func TestAtoiDefault(t *testing.T) {
	if v := atoiDefault("", 7); v != 7 {
		t.Fatalf("atoiDefault empty failed: got %d", v)
	}
	if v := atoiDefault("3", 7); v != 3 {
		t.Fatalf("atoiDefault parse failed: got %d", v)
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	// set minimal required envs to prevent LoadConfig exiting
	os.Setenv("MONGO_URI", "mongodb://localhost:27017")
	os.Setenv("BACKUP_PATH", "./tmp_backups")
	os.Setenv("MONGODUMP_PATH", "mongodump")
	os.Setenv("MONGORESTORE_PATH", "mongorestore")
	os.Setenv("BACKUP_DAYS_INTERVAL", "2")
	os.Setenv("VERBOSE", "1")
	defer func() {
		os.Unsetenv("MONGO_URI")
		os.Unsetenv("BACKUP_PATH")
		os.Unsetenv("MONGODUMP_PATH")
		os.Unsetenv("MONGORESTORE_PATH")
		os.Unsetenv("BACKUP_DAYS_INTERVAL")
		os.Unsetenv("VERBOSE")
	}()
	LoadConfig()
	if AppConfig.BackupDaysInterval != 2 {
		t.Fatalf("Expected BackupDaysInterval 2, got %d", AppConfig.BackupDaysInterval)
	}
	if !AppConfig.Verbose {
		t.Fatalf("Expected Verbose true")
	}
}
