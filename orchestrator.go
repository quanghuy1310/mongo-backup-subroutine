package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// BackupCollectionSingle performs backup for a specific collection (any name)
func BackupCollectionSingle(dbName, collName string) BackupResult {
	result := BackupResult{
		Database:   dbName,
		Collection: collName,
		Status:     StatusFailed,
	}

	// Create directory: AppConfig.BackupPath/<dbName>/<collName>
	dir := filepath.Join(AppConfig.BackupPath, dbName, collName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		Error.Printf("Failed to create backup directory: %v", err)
		result.Error = err
		return result
	}

	Info.Printf("Start backup: DB=%s Collection=%s", dbName, collName)

	// Check if already backed up
	done, err := IsBackupDone(dbName, collName)
	if err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=%v", dbName, collName, err)
		result.Error = err
		return result
	}
	if done {
		reason := "already exists"
		Info.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", dbName, collName, reason)
		result.Status = StatusSkipped
		result.SkipReason = reason
		result.Error = fmt.Errorf("skipped:%s", reason)
		return result
	}

	// Run mongodump with timeout
	ctx, cancel := context.WithTimeout(context.Background(), AppConfig.BackupTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, AppConfig.MongodumpPath,
		"--uri", AppConfig.MongoURI,
		"--db", dbName,
		"--collection", collName,
		"--out", dir,
	)
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=timeout", dbName, collName)
		SaveBackupStatus(dbName, collName, string(StatusFailed), "timeout")
		result.Error = ctx.Err()
		return result
	}

	if err != nil {
		outStr := string(output)
		if strings.Contains(outStr, "ns not found") || strings.Contains(outStr, fmt.Sprintf("collection '%s' does not exist", collName)) {
			reason := "collection not found"
			Info.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", dbName, collName, reason)
			result.Status = StatusSkipped
			result.SkipReason = reason
			SaveBackupStatus(dbName, collName, string(StatusSkipped), reason)
			result.Error = fmt.Errorf("skipped:%s", reason)
			return result
		}
		Error.Printf("Backup failed: DB=%s Collection=%s Error=%v Output=%s", dbName, collName, err, outStr)
		SaveBackupStatus(dbName, collName, string(StatusFailed), outStr)
		result.Error = fmt.Errorf("%v (output: %s)", err, outStr)
		return result
	}

	// Determine produced files
	bsonFile := filepath.Join(dir, dbName, collName+".bson")
	metaFile := filepath.Join(dir, dbName, collName+".metadata.json")
	s2BsonFile := bsonFile + ".s2"
	s2MetaFile := metaFile + ".s2"

	// Ensure dump actually produced files
	if _, err := os.Stat(bsonFile); os.IsNotExist(err) {
		reason := "no data dumped"
		Info.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", dbName, collName, reason)
		result.Status = StatusSkipped
		result.SkipReason = reason
		SaveBackupStatus(dbName, collName, string(StatusSkipped), reason)
		result.Error = fmt.Errorf("skipped:%s", reason)
		return result
	}

	// Check BSON integrity
	if err := CheckBsonIntegrity(bsonFile); err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=BSON integrity check failed %v", dbName, collName, err)
		SaveBackupStatus(dbName, collName, string(StatusFailed), "BSON integrity failed")
		result.Error = err
		return result
	}

	// Check metadata.json validity
	if err := CheckMetadataIntegrity(metaFile); err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=metadata integrity check failed %v", dbName, collName, err)
		SaveBackupStatus(dbName, collName, string(StatusFailed), "metadata integrity failed")
		result.Error = err
		return result
	}

	// Compress files
	filesToCompress := map[string]string{
		bsonFile: s2BsonFile,
		metaFile: s2MetaFile,
	}
	if err := CompressFilesS2(filesToCompress); err != nil {
		Error.Printf("Backup failed: DB=%s Collection=%s Error=compress error %v", dbName, collName, err)
		result.Error = err
		SaveBackupStatus(dbName, collName, string(StatusFailed), "compress error")
		return result
	}

	if info, err := os.Stat(s2BsonFile); err == nil {
		result.FileSizeMB = info.Size() / (1024 * 1024)
	}
	result.BsonFile = s2BsonFile
	result.MetaFile = s2MetaFile
	result.Status = StatusSuccess
	result.Error = nil

	if metaErr := SaveBackupHistory(dbName, collName, s2BsonFile, s2MetaFile, result.FileSizeMB, string(StatusSuccess), "s2", "OK"); metaErr != nil {
		Error.Printf("Failed to save backup metadata: %v", metaErr)
	}

	SaveBackupStatus(dbName, collName, string(StatusSuccess), "OK")
	Info.Printf("Backup success: DB=%s Collection=%s File=%s Size=%d", dbName, collName, s2BsonFile, result.FileSizeMB)

	// Cleanup raw files
	if !AppConfig.KeepRawFiles {
		os.Remove(bsonFile)
		os.Remove(metaFile)
	}

	return result
}

// BackupEntireDatabase backups every collection in the given DB (iterates collections)
func BackupEntireDatabase(dbName string) {
	if mongoClient == nil {
		Error.Printf("mongoClient is nil, cannot backup database %s", dbName)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cols, err := mongoClient.Database(dbName).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		Error.Printf("Failed to list collections for %s: %v", dbName, err)
		return
	}
	Info.Printf("Backing up %d collections for DB=%s", len(cols), dbName)
	for _, c := range cols {
		Info.Printf("Backing up %s.%s", dbName, c)
		res := BackupCollectionSingle(dbName, c)
		if res.Error != nil {
			if strings.HasPrefix(res.Error.Error(), "skipped:") {
				Warn.Printf("Skipped %s.%s: %s", dbName, c, res.SkipReason)
			} else {
				Error.Printf("Failed to backup %s.%s: %v", dbName, c, res.Error)
			}
		}
	}
}
