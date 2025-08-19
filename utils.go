package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"path/filepath"

	"github.com/klauspost/compress/s2"
)

func FormatDate(t time.Time) string {
	return t.Format("2006_01_02")
}

func BackupDir(dbName string, date time.Time) string {
	// Parent folder: YYYY_ProviderID, subfolder: GPS_YYYY_MM_DD
	parent := fmt.Sprintf("%s/%s", AppConfig.BackupPath, dbName)
	collection := fmt.Sprintf("GPS_%s", FormatDate(date))
	dir := fmt.Sprintf("%s/%s", parent, collection)
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create backup directory: %v\n", err)
		return ""
	}
	return dir
}

// Decompress s2 file to BSON
func DecompressFileS2(srcPath, dstPath string) error {
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
	reader := s2.NewReader(in)
	_, err = io.Copy(out, reader)
	return err
}

// Bulk restore from a list of s2 backup files
func BulkRestore(restoreList []string, dbName, collection string) {
	for _, s2BsonFile := range restoreList {
		bsonFile := s2BsonFile[:len(s2BsonFile)-3]                // remove .s2 extension
		metaFile := bsonFile[:len(bsonFile)-5] + ".metadata.json" // replace .bson with .metadata.json
		s2MetaFile := metaFile + ".s2"
		// Giải nén cả hai file
		errBson := DecompressFileS2(s2BsonFile, bsonFile)
		errMeta := DecompressFileS2(s2MetaFile, metaFile)
		if errBson != nil || errMeta != nil {
			fmt.Fprintf(os.Stderr, "Failed to decompress %s or %s: %v %v\n", s2BsonFile, s2MetaFile, errBson, errMeta)
			continue
		}
		// Run mongorestore trỏ vào folder chứa cả hai file
		folder := filepath.Dir(bsonFile)
		cmd := exec.Command(AppConfig.MongodumpPath[:len(AppConfig.MongodumpPath)-4]+"restore",
			"--uri", AppConfig.MongoURI,
			"--db", dbName,
			"--collection", collection,
			folder,
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "mongorestore failed for %s: %v\nOutput: %s\n", folder, err, string(output))
		} else {
			fmt.Printf("[INFO] Restore successful for %s\n", folder)
		}
	}
}
