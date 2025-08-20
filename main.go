package main

import (
	"time"
)

func main() {
	LoadConfig()
	InitLogger(AppConfig.LogFile)
	Info.Println("Mongo Backup Subroutine v2.1 starting...")

	if err := ConnectMongo(AppConfig.MongoURI); err != nil {
		Error.Printf("Failed to connect MongoDB: %v", err)
		return
	}
	defer DisconnectMongo()

	for {
		now := time.Now()
		scheduled := time.Date(now.Year(), now.Month(), now.Day(), AppConfig.ScheduleHour, AppConfig.ScheduleMin, 0, 0, now.Location())
		if now.After(scheduled) {
			scheduled = scheduled.Add(24 * time.Hour)
		}
		sleepDuration := time.Until(scheduled)
		Info.Printf("Next scheduled backup at %s (sleep %s)", scheduled.Format("2006-01-02 15:04:05"), sleepDuration)
		time.Sleep(sleepDuration)

		backupDate := time.Now().AddDate(0, 0, -1) // backup hôm qua
		RunFullBackup(backupDate)
	}
}
