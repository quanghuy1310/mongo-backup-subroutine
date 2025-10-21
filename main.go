package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	LoadConfig() // load .env

	// Init logger
	if err := InitLogger(AppConfig.LogFile); err != nil {
		log.Fatalf("Failed to init logger: %v", err)
	}

	// If CLI args exist -> run Cobra CLI
	if len(os.Args) > 1 {
		// Ensure Mongo connection is available for CLI commands that need it
		if err := ConnectMongo(AppConfig.MongoURI); err != nil {
			Error.Printf("Failed to connect MongoDB for CLI: %v", err)
			log.Fatalf("Failed to connect MongoDB: %v", err)
		}
		defer DisconnectMongo()

		Execute()
		return
	}

	// Otherwise run as scheduled service
	Info.Println("Mongo Backup Subroutine v2.2 starting...")

	if err := ConnectMongo(AppConfig.MongoURI); err != nil {
		Error.Printf("Failed to connect MongoDB: %v", err)
		os.Exit(1)
	}
	defer DisconnectMongo()

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		Info.Printf("Received signal: %s, shutting down...", sig)
		cancel()
	}()

	// Run scheduled tasks (blocking until ctx canceled)
	safeRunScheduledTasks(ctx)
}

// safeRunScheduledTasks runs the scheduled backup + retention loop and recovers panics
func safeRunScheduledTasks(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			Error.Printf("panic in scheduled tasks: %v", r)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			Info.Println("Shutdown requested, exiting scheduled tasks")
			CloseLogger()
			return
		default:
		}

		now := time.Now()
		scheduled := time.Date(now.Year(), now.Month(), now.Day(),
			AppConfig.ScheduleHour, AppConfig.ScheduleMin, 0, 0, now.Location())
		if now.After(scheduled) {
			scheduled = scheduled.Add(24 * time.Hour)
		}
		sleepDuration := time.Until(scheduled)
		Info.Printf("Next scheduled backup at %s (sleep %s)",
			scheduled.Format("2006-01-02 15:04:05"), sleepDuration)

		select {
		case <-time.After(sleepDuration):
			// proceed
		case <-ctx.Done():
			Info.Println("Shutdown requested before next scheduled run")
			CloseLogger()
			return
		}

		// Yesterday backup
		backupDate := time.Now().AddDate(0, 0, -1)
		RunFullBackup(backupDate)

		// Retention
		retentionDays := AppConfig.RetentionDays
		backupBasePath := AppConfig.BackupPath
		Info.Printf("Running retention policy: basePath=%s, retentionDays=%d", backupBasePath, retentionDays)
		if err := DeleteOldBackups(backupBasePath, retentionDays); err != nil {
			Error.Printf("Retention process failed: %v", err)
		} else {
			Info.Println("Retention policy completed successfully.")
		}
	}
}
