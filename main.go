package main

import (
	"log"
	"os"
	"time"
)

func main() {
	LoadConfig() // load .env trước

	// Khởi tạo logger, fatal nếu fail
	if err := InitLogger(AppConfig.LogFile); err != nil {
		log.Fatalf("Failed to init logger: %v", err)
	}
	Info.Println("Mongo Backup Subroutine v2.2 starting...")

	// Kết nối MongoDB
	if err := ConnectMongo(AppConfig.MongoURI); err != nil {
		Error.Printf("Failed to connect MongoDB: %v", err)
		os.Exit(1)
	}
	defer DisconnectMongo()

	// Backup định kỳ hằng ngày vào thời điểm AppConfig.ScheduleHour:ScheduleMin
	for {
		now := time.Now()
		scheduled := time.Date(now.Year(), now.Month(), now.Day(),
			AppConfig.ScheduleHour, AppConfig.ScheduleMin, 0, 0, now.Location())
		if now.After(scheduled) {
			scheduled = scheduled.Add(24 * time.Hour)
		}
		sleepDuration := time.Until(scheduled)
		Info.Printf("Next scheduled backup at %s (sleep %s)",
			scheduled.Format("2006-01-02 15:04:05"), sleepDuration)
		time.Sleep(sleepDuration)

		// Backup hôm qua
		backupDate := time.Now().AddDate(0, 0, -1)
		RunFullBackup(backupDate)
	}
}
