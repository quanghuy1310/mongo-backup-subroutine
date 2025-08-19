package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/s2"
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
	cmd := exec.Command(AppConfig.MongodumpPath,
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

	// Nén file BSON và metadata.json bằng s2
	bsonFile := filepath.Join(dir, dbName, collection+".bson")
	metaFile := filepath.Join(dir, dbName, collection+".metadata.json")
	s2BsonFile := bsonFile + ".s2"
	s2MetaFile := metaFile + ".s2"
	compressBsonErr := CompressFileS2(bsonFile, s2BsonFile)
	_ = CompressFileS2(metaFile, s2MetaFile) // ignore error for now, can log if needed
	var fileSize int64
	if compressBsonErr == nil {
		info, err := os.Stat(s2BsonFile)
		if err == nil {
			fileSize = info.Size()
		}
	}

	// Lưu metadata vào MongoDB
	metaErr := SaveBackupHistory(dbName, collection, s2BsonFile, fileSize, "success", "s2", "OK")
	if metaErr != nil {
		fmt.Printf("[ERROR] Failed to save backup metadata: %v\n", metaErr)
	}

	SaveBackupStatus(dbName, FormatDate(date), "success", "OK")
	fmt.Printf("[INFO] Backup successful: %s, collection: %s\n", dbName, collection)
	return nil
}

// Compress BSON file to s2 format
func CompressFileS2(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer out.Close()
	writer := s2.NewWriter(out)
	_, err = io.Copy(writer, in)
	writer.Close()
	return err
}

// Save backup metadata to MongoDB
func SaveBackupHistory(dbName, collection, filePath string, fileSize int64, status, compression, msg string) error {
	coll := mongoClient.Database("admin").Collection("backupHistory")
	_, err := coll.InsertOne(context.Background(), map[string]interface{}{
		"database":    dbName,
		"collection":  collection,
		"filePath":    filePath,
		"fileSize":    fileSize,
		"status":      status,
		"compression": compression,
		"message":     msg,
		"timestamp":   time.Now(),
	})
	return err
}

func BackupWithRetry(dbName string, date time.Time) error {
	var lastErr error
	for i := 0; i < AppConfig.MaxRetries; i++ {
		err := BackupDatabase(dbName, date)
		if err == nil {
			return nil
		}
		lastErr = err
		fmt.Println("Backup failed, retrying after:", AppConfig.RetryInterval)
		time.Sleep(AppConfig.RetryInterval)
	}
	fmt.Println("Backup failed after max retries:", dbName, date)
	return lastErr
}
