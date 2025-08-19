package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

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
	for _, s2File := range restoreList {
		bsonFile := s2File[:len(s2File)-3] // remove .s2 extension
		err := DecompressFileS2(s2File, bsonFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to decompress %s: %v\n", s2File, err)
			continue
		}
		// Run mongorestore
		cmd := exec.Command(AppConfig.MongodumpPath[:len(AppConfig.MongodumpPath)-4]+"restore", // replace mongodump with mongorestore
			"--uri", AppConfig.MongoURI,
			"--db", dbName,
			"--collection", collection,
			bsonFile,
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "mongorestore failed for %s: %v\nOutput: %s\n", bsonFile, err, string(output))
		} else {
			fmt.Printf("[INFO] Restore successful for %s\n", bsonFile)
		}
	}
}
