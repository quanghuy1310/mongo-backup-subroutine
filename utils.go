package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/s2"
)

// FormatDate returns YYYY_MM_DD
func FormatDate(t time.Time) string {
	return t.Format("2006_01_02")
}

// BackupDir returns backup folder path, creates it if missing
func BackupDir(dbName string, date time.Time) (string, error) {
	dir := filepath.Join(AppConfig.BackupPath, dbName, fmt.Sprintf("GPS_%s", FormatDate(date)))
	if err := os.MkdirAll(dir, 0755); err != nil {
		Error.Printf("Failed to create backup directory: %v", err)
		return "", err
	}
	Info.Printf("Backup directory ready: %s", dir)
	return dir, nil
}

// CompressFilesS2 compress multiple files to .s2 format
func CompressFilesS2(files map[string]string) error {
	for src, dst := range files {
		in, err := os.Open(src)
		if err != nil {
			return fmt.Errorf("failed to open %s: %w", src, err)
		}
		out, err := os.Create(dst)
		if err != nil {
			in.Close()
			return fmt.Errorf("failed to create %s: %w", dst, err)
		}

		writer := s2.NewWriter(out)
		buf := make([]byte, 1<<20)
		if _, err := io.CopyBuffer(writer, in, buf); err != nil {
			writer.Close()
			in.Close()
			out.Close()
			return fmt.Errorf("failed to compress %s: %w", src, err)
		}

		writer.Close()
		in.Close()
		out.Close()
		Info.Printf("Compressed %s -> %s", src, dst)
	}
	return nil
}

// DecompressFileS2 decompress a .s2 file
func DecompressFileS2(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		Error.Printf("Failed to open %s: %v", srcPath, err)
		return err
	}
	defer in.Close()

	out, err := os.Create(dstPath)
	if err != nil {
		Error.Printf("Failed to create %s: %v", dstPath, err)
		return err
	}
	defer out.Close()

	reader := s2.NewReader(in)
	if _, err := io.Copy(out, reader); err != nil {
		Error.Printf("Failed to decompress %s -> %s: %v", srcPath, dstPath, err)
		return err
	}

	Info.Printf("Decompressed %s -> %s", srcPath, dstPath)
	return nil
}

// CheckBsonIntegrity validates BSON file using bsondump --quiet
func CheckBsonIntegrity(bsonPath string) error {
	if _, err := os.Stat(bsonPath); os.IsNotExist(err) {
		return fmt.Errorf("bson file does not exist: %s", bsonPath)
	}

	cmd := exec.Command("bsondump", "--quiet", bsonPath)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("bson integrity check failed: %v", err)
	}
	return nil
}

// CheckMetadataIntegrity validates metadata.json
func CheckMetadataIntegrity(metaPath string) error {
	f, err := os.Open(metaPath)
	if err != nil {
		return fmt.Errorf("metadata file open failed: %v", err)
	}
	defer f.Close()

	var tmp map[string]interface{}
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&tmp); err != nil {
		return fmt.Errorf("metadata invalid JSON: %v", err)
	}
	return nil
}

// SaveBackupHistory inserts backup record into MongoDB
func SaveBackupHistory(dbName, collection, bsonFile, metaFile string, fileSize int64, status, compression, msg string) error {
	coll := mongoClient.Database("admin").Collection("backupHistory")
	_, err := coll.InsertOne(context.Background(), map[string]interface{}{
		"database":    dbName,
		"collection":  collection,
		"bsonFile":    bsonFile,
		"metaFile":    metaFile,
		"fileSize":    fileSize,
		"status":      status,
		"compression": compression,
		"message":     msg,
		"timestamp":   time.Now(),
	})
	return err
}

// BulkRestore restores multiple .s2 backup files into MongoDB
func BulkRestore(restoreList []string, dbName, collection string) {
	for _, s2BsonFile := range restoreList {
		bsonFile := s2BsonFile[:len(s2BsonFile)-3]                // remove .s2
		metaFile := bsonFile[:len(bsonFile)-5] + ".metadata.json" // replace .bson
		s2MetaFile := metaFile + ".s2"

		// Decompress
		if err := DecompressFileS2(s2BsonFile, bsonFile); err != nil {
			Error.Printf("Failed to decompress BSON: %s -> %s", s2BsonFile, bsonFile)
			continue
		}
		if err := DecompressFileS2(s2MetaFile, metaFile); err != nil {
			Warn.Printf("Failed to decompress metadata: %s -> %s", s2MetaFile, metaFile)
		}

		restoreFolder := filepath.Dir(bsonFile)
		restoreCmdPath := AppConfig.MongodumpPath[:len(AppConfig.MongodumpPath)-4] + "restore"
		cmd := exec.Command(restoreCmdPath,
			"--uri", AppConfig.MongoURI,
			"--db", dbName,
			"--collection", collection,
			"--drop",
			restoreFolder,
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			Error.Printf("mongorestore failed for %s: %v\nOutput: %s", restoreFolder, err, string(output))
			continue
		}
		Info.Printf("Restore successful for %s (BSON + metadata)", restoreFolder)

		if !AppConfig.KeepRawFiles {
			os.Remove(bsonFile)
			os.Remove(metaFile)
			Info.Printf("Cleaned up raw files for %s", restoreFolder)
		}
	}
}
