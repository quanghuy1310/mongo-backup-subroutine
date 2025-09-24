package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// Root command
var rootCmd = &cobra.Command{
	Use:   "mongobackup",
	Short: "MongoDB Backup Subroutine CLI",
	Long:  "MongoDB Backup Subroutine - manage MongoDB backup and restore operations",
}

// ------------------ BACKUP COMMAND ------------------
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup a database or a collection",
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := cmd.Flags().GetString("db")
		coll, _ := cmd.Flags().GetString("collection")
		allDBs, _ := cmd.Flags().GetBool("all")
		dateStr, _ := cmd.Flags().GetString("date")

		var backupDate time.Time
		if dateStr == "" {
			backupDate = time.Now().AddDate(0, 0, -1) // default yesterday
		} else {
			var err error
			backupDate, err = time.Parse("2006-01-02", dateStr)
			if err != nil {
				fmt.Println("Invalid date format, expected YYYY-MM-DD")
				os.Exit(1)
			}
		}

		if allDBs {
			Info.Println("Starting backup for all databases ...")
			RunFullBackup(backupDate)
			return
		}

		if db == "" {
			fmt.Println("You must provide --db or --all")
			os.Exit(1)
		}

		if coll != "" {
			Info.Printf("Start backup: DB=%s Collection=%s Date=%s", db, coll, backupDate.Format("2006-01-02"))

			result := BackupDatabase(db, backupDate) // <- trả về BackupResult

			if result.Error != nil {
				// Phân biệt lỗi skip hay thực sự failed
				if strings.HasPrefix(result.Error.Error(), "skipped:") {
					Warn.Printf("Backup skipped: DB=%s Collection=%s Reason=%s", db, result.Collection, result.SkipReason)
				} else {
					Error.Printf("Backup failed: DB=%s Collection=%s Error=%v", db, result.Collection, result.Error)
				}
				return
			}

			Info.Printf("Backup completed: DB=%s Collection=%s Date=%s", db, result.Collection, backupDate.Format("2006-01-02"))
		} else {
			Info.Printf("Backing up entire database %s ...", db)
			RunFullBackup(backupDate)
		}
	},
}

// ------------------ RESTORE COMMAND ------------------
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a database or a collection",
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := cmd.Flags().GetString("db")
		coll, _ := cmd.Flags().GetString("collection")
		file, _ := cmd.Flags().GetString("file")
		allDBs, _ := cmd.Flags().GetBool("all")
		sandbox, _ := cmd.Flags().GetBool("sandbox")

		if allDBs {
			Info.Println("Restoring all databases ...")
			// TODO: implement BulkRestoreAllDatabases if needed
			return
		}

		if db == "" {
			fmt.Println("You must provide --db or --all")
			os.Exit(1)
		}

		restoreDB := db
		if sandbox {
			restoreDB = db + "_sandbox" // sandbox DB
			Info.Printf("Restoring into sandbox database: %s", restoreDB)
		}

		if coll != "" {
			if file == "" {
				fmt.Println("You must provide --file to restore a collection")
				os.Exit(1)
			}

			bsonFile := file[:len(file)-3] // remove .s2
			metaFile := bsonFile[:len(bsonFile)-5] + ".metadata.json"
			s2MetaFile := metaFile + ".s2"

			Info.Printf("Start restore: DB=%s Collection=%s File=%s", restoreDB, coll, file)
			Info.Printf("Decompressing BSON file: %s ...", file)
			if err := DecompressFileS2(file, bsonFile); err != nil {
				Error.Printf("Failed to decompress BSON: %v", err)
				os.Exit(1)
			}
			Info.Printf("Decompress BSON completed: %s", bsonFile)

			Info.Printf("Decompressing metadata file: %s ...", s2MetaFile)
			if err := DecompressFileS2(s2MetaFile, metaFile); err != nil {
				Warn.Printf("Failed to decompress metadata: %v", err)
			} else {
				Info.Printf("Decompress metadata completed: %s", metaFile)
			}

			Info.Printf("Restoring collection: %s.%s ...", restoreDB, coll)
			BulkRestore([]string{file}, restoreDB, coll)
			Info.Printf("Restore completed: DB=%s Collection=%s", restoreDB, coll)

		} else {
			if file == "" {
				fmt.Println("You must provide --file to restore a database")
				os.Exit(1)
			}

			Info.Printf("Restoring entire database %s from folder %s ...", restoreDB, file)

			entries, err := os.ReadDir(file)
			if err != nil {
				Error.Printf("Failed to read folder: %v", err)
				os.Exit(1)
			}

			var files []string
			for _, e := range entries {
				if !e.IsDir() && len(e.Name()) > 3 && filepath.Ext(e.Name()) == ".s2" {
					files = append(files, filepath.Join(file, e.Name()))
				}
			}

			if len(files) == 0 {
				Warn.Printf("No .s2 files found in folder %s", file)
				return
			}

			for idx, f := range files {
				Info.Printf("[%d/%d] Restoring file %s ...", idx+1, len(files), f)
			}

			BulkRestore(files, restoreDB, "")
			Info.Printf("Restore completed: DB=%s (all collections)", restoreDB)
		}
	},
}

// ------------------ EXECUTE ------------------
func Execute() {
	// Backup flags
	backupCmd.Flags().String("db", "", "Database name")
	backupCmd.Flags().String("collection", "", "Collection name")
	backupCmd.Flags().Bool("all", false, "Backup all databases")
	backupCmd.Flags().String("date", "", "Backup date YYYY-MM-DD (default yesterday)")

	// Restore flags
	restoreCmd.Flags().String("db", "", "Database name")
	restoreCmd.Flags().String("collection", "", "Collection name")
	restoreCmd.Flags().String("file", "", "Path to backup file (.bson.s2)")
	restoreCmd.Flags().Bool("all", false, "Restore all databases")
	restoreCmd.Flags().Bool("sandbox", false, "Restore into sandbox database (DB_sandbox)")

	// Add commands to root
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
