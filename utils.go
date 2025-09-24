package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
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
			return nil // continue walk
		}
		if !d.IsDir() {
			return nil
		}

		name := d.Name()
		matches := re.FindStringSubmatch(name)
		if matches == nil {
			return nil // skip non-matching folders
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
				return filepath.SkipDir // Skip already deleted folder
			}
		} else {
			// Info.Printf("Keeping folder: %s (date=%s)", path, folderDate.Format("2006-01-02"))
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

// FindBackupFiles locates BSON and metadata backup files for a collection
func FindBackupFiles(dbName, collName string) (string, string, error) {
	// Folder dạng: <BackupPath>/<db>/<collection>/<db>/
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

// RestoreCollectionWithMetadata restores a single collection with its metadata
// RestoreCollectionWithMetadata restores a collection from bson.s2 + metadata.json.s2
// It will decompress directly inside BACKUP_PATH and clean up if keepRawFile=false
// RestoreCollectionWithMetadata restores a single collection with its metadata
// CHANGED: added parameter verify bool to optionally check integrity before mongorestore
func RestoreCollectionWithMetadata(bsonFile, metaFile, db, coll string, verify bool) error {
	// thư mục chứa file .s2
	dir := filepath.Dir(bsonFile)

	rawBson := strings.TrimSuffix(bsonFile, ".s2") // -> .bson
	rawMeta := strings.TrimSuffix(metaFile, ".s2") // -> .metadata.json

	// giải nén
	if err := DecompressFileS2(bsonFile, rawBson); err != nil {
		return fmt.Errorf("failed to decompress bson: %w", err)
	}
	if err := DecompressFileS2(metaFile, rawMeta); err != nil {
		return fmt.Errorf("failed to decompress metadata: %w", err)
	}

	// CHANGED: Optional integrity verification BEFORE mongorestore
	if verify {
		if err := CheckBsonIntegrity(rawBson); err != nil {
			// nếu verify fail -> cleanup raw files (nếu cần) và trả lỗi
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

	// chạy mongorestore, trỏ trực tiếp vào folder (đúng cấu trúc mongodump)
	cmd := exec.Command("mongorestore", "--drop", "--db", db, dir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// nếu thất bại, giữ raw file nếu config yêu cầu, trả lỗi kèm output
		return fmt.Errorf("mongorestore failed: %v\noutput=%s", err, string(out))
	}

	// nếu config không giữ raw file thì xóa đi
	if !AppConfig.KeepRawFiles {
		os.Remove(rawBson)
		os.Remove(rawMeta)
	}

	return nil
}

// RestoreDatabase restores all GPS_* collections from a database folder
// CHANGED: wrapper to restore multiple collections with sandbox + verify support
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

	for idx, c := range collFolders {
		bsonFile, metaFile, err := FindBackupFiles(originalDB, c)
		if err != nil {
			Error.Printf("Cannot find backup files for %s: %v", c, err)
			continue
		}
		Info.Printf("[%d/%d] Restoring collection %s into DB=%s (verify=%v)...", idx+1, len(collFolders), c, restoreDB, verify)
		if err := RestoreCollectionWithMetadata(bsonFile, metaFile, restoreDB, c, verify); err != nil {
			Error.Printf("Restore failed for %s: %v", c, err)
		}
	}
	Info.Printf("Restore completed: DB=%s (all collections)", restoreDB)
	return nil
}

// BulkRestore restores multiple .s2 backup files into MongoDB
// CHANGED: added verify flag and support sandbox (dbName can be db_sandbox)
func BulkRestore(restoreList []string, dbName, collection string, verify bool) {
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

		// CHANGED: verify integrity if requested
		if verify {
			if err := CheckBsonIntegrity(bsonFile); err != nil {
				Error.Printf("Integrity check failed for BSON %s: %v", bsonFile, err)
				continue
			}
			if err := CheckMetadataIntegrity(metaFile); err != nil {
				Error.Printf("Integrity check failed for Metadata %s: %v", metaFile, err)
				continue
			}
			Info.Printf("Integrity check passed for %s.%s", dbName, collection)
		}

		restoreFolder := filepath.Dir(bsonFile)
		restoreCmdPath := AppConfig.MongodumpPath[:len(AppConfig.MongodumpPath)-4] + "restore"
		cmd := exec.Command(restoreCmdPath,
			"--uri", AppConfig.MongoURI,
			"--db", dbName, // dbName có thể là sandbox (db_sandbox)
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
