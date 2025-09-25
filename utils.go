package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

// DeleteOldBackups deletes backup folders older than retentionDays
func DeleteOldBackups(baseDir string, retentionDays int) error {
	if retentionDays <= 0 {
		Info.Printf("RetentionDays <= 0, skip deleting old backups")
		return nil
	}

	re := regexp.MustCompile(`^GPS_(\d{4})_(\d{2})_(\d{2})$`)
	cutoffDate := time.Now().AddDate(0, 0, -retentionDays)

	var deletedCount, keptCount int

	err := filepath.WalkDir(baseDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			Error.Printf("Error reading path=%s err=%v", path, err)
			return nil
		}
		if !d.IsDir() {
			return nil
		}

		name := d.Name()
		matches := re.FindStringSubmatch(name)
		if matches == nil {
			return nil
		}

		folderDate, parseErr := time.Parse("2006_01_02", fmt.Sprintf("%s_%s_%s", matches[1], matches[2], matches[3]))
		if parseErr != nil {
			Error.Printf("Failed to parse date from folder=%s err=%v", name, parseErr)
			return nil
		}

		if folderDate.Before(cutoffDate) {
			Warn.Printf("Deleting old backup folder: %s (date=%s < cutoff=%s)",
				path, folderDate.Format("2006-01-02"), cutoffDate.Format("2006-01-02"))
			if rmErr := os.RemoveAll(path); rmErr != nil {
				Error.Printf("Failed to delete folder=%s err=%v", path, rmErr)
			} else {
				Info.Printf("Deleted folder successfully: %s", path)
				deletedCount++
				return filepath.SkipDir
			}
		} else {
			keptCount++
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("error walking baseDir=%s: %w", baseDir, err)
	}

	Info.Printf("Retention summary: deleted=%d kept=%d", deletedCount, keptCount)
	return nil
}

// CHANGED / OPTIMIZED: CompressFilesS2 uses worker pool for parallel compress
func CompressFilesS2(files map[string]string) error {
	type task struct{ src, dst string }
	tasks := make(chan task, len(files))
	for src, dst := range files {
		tasks <- task{src, dst}
	}
	close(tasks)

	workerCount := AppConfig.WorkerCount

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 8<<20) // 8MB buffer
			for t := range tasks {
				in, err := os.Open(t.src)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("failed to open %s: %w", t.src, err)
					}
					mu.Unlock()
					continue
				}
				out, err := os.Create(t.dst)
				if err != nil {
					in.Close()
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("failed to create %s: %w", t.dst, err)
					}
					mu.Unlock()
					continue
				}
				writer := s2.NewWriter(out)
				if _, err := io.CopyBuffer(writer, in, buf); err != nil {
					writer.Close()
					in.Close()
					out.Close()
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("failed to compress %s: %w", t.src, err)
					}
					mu.Unlock()
					continue
				}
				writer.Close()
				in.Close()
				out.Close()
				Info.Printf("Compressed %s -> %s", t.src, t.dst)
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// CHANGED / OPTIMIZED: DecompressFileS2 using larger buffer
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

	buf := make([]byte, 8<<20) // 8MB buffer
	reader := s2.NewReader(in)
	if _, err := io.CopyBuffer(out, reader, buf); err != nil {
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

// FindBackupFiles locates BSON and metadata backup files for a collection
func FindBackupFiles(dbName, collName string) (string, string, error) {
	collFolder := filepath.Join(AppConfig.BackupPath, dbName, collName, dbName)
	info, err := os.Stat(collFolder)
	if err != nil || !info.IsDir() {
		return "", "", fmt.Errorf("backup folder not found: %s", collFolder)
	}

	bsonFile := filepath.Join(collFolder, collName+".bson.s2")
	metaFile := filepath.Join(collFolder, collName+".metadata.json.s2")

	if _, err := os.Stat(bsonFile); err != nil {
		return "", "", fmt.Errorf("BSON file not found: %s", bsonFile)
	}
	if _, err := os.Stat(metaFile); err != nil {
		return "", "", fmt.Errorf("MetaData file not found: %s", metaFile)
	}
	return bsonFile, metaFile, nil
}

// CHANGED / OPTIMIZED: RestoreCollectionWithMetadata uses AppConfig.MongorestorePath, verify optional, cleanup
func RestoreCollectionWithMetadata(bsonFile, metaFile, db, coll string, verify bool) error {
	dir := filepath.Dir(bsonFile)

	rawBson := strings.TrimSuffix(bsonFile, ".s2")
	rawMeta := strings.TrimSuffix(metaFile, ".s2")

	if err := DecompressFileS2(bsonFile, rawBson); err != nil {
		return fmt.Errorf("failed to decompress bson: %w", err)
	}
	if err := DecompressFileS2(metaFile, rawMeta); err != nil {
		return fmt.Errorf("failed to decompress metadata: %w", err)
	}

	if verify {
		Info.Printf("Verifying integrity for %s.%s ...", db, coll)
		if err := CheckBsonIntegrity(rawBson); err != nil {
			if !AppConfig.KeepRawFiles {
				os.Remove(rawBson)
				os.Remove(rawMeta)
			}
			return fmt.Errorf("integrity check failed for BSON: %w", err)
		}
		if err := CheckMetadataIntegrity(rawMeta); err != nil {
			if !AppConfig.KeepRawFiles {
				os.Remove(rawBson)
				os.Remove(rawMeta)
			}
			return fmt.Errorf("integrity check failed for metadata: %w", err)
		}
		Info.Printf("Integrity check passed for %s.%s", db, coll)
	}

	restoreCmdPath := AppConfig.MongorestorePath
	if restoreCmdPath == "" {
		restoreCmdPath = "mongorestore"
	}
	Info.Printf("Running mongorestore for %s.%s into DB=%s ...", db, coll, db)
	cmd := exec.Command(restoreCmdPath,
		"--uri", AppConfig.MongoURI,
		"--drop",
		"--db", db,
		"--numInsertionWorkersPerCollection", strconv.Itoa(AppConfig.WorkerCount),
		dir,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if !AppConfig.KeepRawFiles {
			os.Remove(rawBson)
			os.Remove(rawMeta)
		}
		return fmt.Errorf("mongorestore failed for %s.%s: %v\noutput=%s",
			db, coll, err, string(out))
	}

	Info.Printf("Restore completed for %s.%s", db, coll)

	if !AppConfig.KeepRawFiles {
		os.Remove(rawBson)
		os.Remove(rawMeta)
		Info.Printf("Cleaned up raw files for %s.%s", db, coll)
	}
	return nil
}

// RestoreDatabase restores all collections of a DB using RestoreCollectionWithMetadata
func RestoreDatabase(originalDB, restoreDB string, verify bool) error {
	dbPath := filepath.Join(AppConfig.BackupPath, originalDB)

	entries, err := os.ReadDir(dbPath)
	if err != nil {
		return fmt.Errorf("failed to read folder %s: %w", dbPath, err)
	}

	var collFolders []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "GPS_") {
			collFolders = append(collFolders, e.Name())
		}
	}
	if len(collFolders) == 0 {
		Warn.Printf("No GPS collections found in folder %s", dbPath)
		return nil
	}

	for _, c := range collFolders {
		bsonFile, metaFile, err := FindBackupFiles(originalDB, c)
		if err != nil {
			Error.Printf("Cannot find backup files for %s: %v", c, err)
			continue
		}
		Info.Printf("Restoring collection %s into DB=%s (verify=%v)...", c, restoreDB, verify)
		if err := RestoreCollectionWithMetadata(bsonFile, metaFile, restoreDB, c, verify); err != nil {
			Error.Printf("Restore failed for %s: %v", c, err)
		}
	}

	Info.Printf("Restore completed: DB=%s (all collections)", restoreDB)
	return nil
}

// BulkRestore restores a list of BSON+metadata files into a DB/collection
// using RestoreCollectionWithMetadata (no outer worker pool needed).
func BulkRestore(restoreList []string, dbName, collection string, verify bool) {
	if len(restoreList) == 0 {
		Warn.Printf("No files provided for bulk restore into %s.%s", dbName, collection)
		return
	}

	for _, s2BsonFile := range restoreList {
		bsonFile := s2BsonFile
		metaFile := strings.TrimSuffix(s2BsonFile, ".bson.s2") + ".metadata.json.s2"

		Info.Printf("Restoring file %s into %s.%s (verify=%v)...", bsonFile, dbName, collection, verify)
		if err := RestoreCollectionWithMetadata(bsonFile, metaFile, dbName, collection, verify); err != nil {
			Error.Printf("Bulk restore failed for %s: %v", bsonFile, err)
		} else {
			Info.Printf("Bulk restore success for %s -> %s.%s", bsonFile, dbName, collection)
		}
	}

	Info.Printf("Bulk restore completed for DB=%s Collection=%s", dbName, collection)
}
