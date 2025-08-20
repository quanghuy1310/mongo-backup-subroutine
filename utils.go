package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/s2"
)

// FormatDate trả về ngày theo định dạng YYYY_MM_DD
func FormatDate(t time.Time) string {
	return t.Format("2006_01_02")
}

// BackupDir trả về đường dẫn backup chuẩn: /<BackupPath>/<DB>/<Collection>/
// Tạo folder nếu chưa tồn tại, không tạo thêm 1 cấp con thừa
func BackupDir(dbName string, date time.Time) string {
	dir := filepath.Join(AppConfig.BackupPath, dbName, fmt.Sprintf("GPS_%s", FormatDate(date)))

	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Failed to create backup directory: %v\n", err)
		return ""
	}

	return dir
}

// CompressFilesS2 nén nhiều file cùng lúc sang định dạng .s2
// files: map[srcPath]dstPath
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
	}
	return nil
}

// DecompressFileS2 giải nén một file .s2 về định dạng gốc (BSON/JSON)
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

// BulkRestore thực hiện restore từ danh sách file backup .s2
// BulkRestore restores multiple .s2 backup files into MongoDB
func BulkRestore(restoreList []string, dbName, collection string) {
	for _, s2BsonFile := range restoreList {
		// Determine original file names
		bsonFile := s2BsonFile[:len(s2BsonFile)-3]                // remove .s2
		metaFile := bsonFile[:len(bsonFile)-5] + ".metadata.json" // replace .bson
		s2MetaFile := metaFile + ".s2"

		// Decompress BSON
		if err := DecompressFileS2(s2BsonFile, bsonFile); err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Failed to decompress BSON: %s -> %s : %v\n", s2BsonFile, bsonFile, err)
			continue
		}

		// Decompress metadata (optional, warn if fails)
		if err := DecompressFileS2(s2MetaFile, metaFile); err != nil {
			fmt.Fprintf(os.Stderr, "[WARN] Failed to decompress metadata: %s -> %s : %v\n", s2MetaFile, metaFile, err)
		}

		// Folder containing both BSON + metadata
		restoreFolder := filepath.Dir(bsonFile)

		// Run mongorestore, point to folder, let mongorestore detect metadata automatically
		restoreCmdPath := AppConfig.MongodumpPath[:len(AppConfig.MongodumpPath)-4] + "restore"
		cmd := exec.Command(restoreCmdPath,
			"--uri", AppConfig.MongoURI,
			"--db", dbName,
			"--collection", collection,
			"--drop",      // drop collection before restore
			restoreFolder, // pass folder
		)

		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] mongorestore failed for %s: %v\nOutput: %s\n", restoreFolder, err, string(output))
			continue
		}

		fmt.Printf("[INFO] Restore successful for %s (BSON + metadata)\n", restoreFolder)

		// Optional cleanup: remove decompressed files if configured
		if !AppConfig.KeepRawFiles {
			os.Remove(bsonFile)
			os.Remove(metaFile)
		}
	}
}
