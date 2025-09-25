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
			result := BackupDatabase(db, backupDate)
			if result.Error != nil {
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
		verify, _ := cmd.Flags().GetBool("verify")

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
			restoreDB = db + "_sandbox"
			Info.Printf("Restoring into sandbox database: %s", restoreDB)
		}

		if coll != "" {
			var bsonFile, metaFile string
			var err error
			if file != "" {
				bsonFile = file
				metaFile = bsonFile[:len(bsonFile)-len(".bson.s2")] + ".metadata.json.s2"
			} else {
				bsonFile, metaFile, err = FindBackupFiles(db, coll)
				if err != nil {
					Error.Printf("Cannot find backup files: %v", err)
					os.Exit(1)
				}
			}
			Info.Printf("Restoring collection %s.%s (with metadata) into DB=%s (verify=%v)", db, coll, restoreDB, verify)
			if err := RestoreCollectionWithMetadata(bsonFile, metaFile, restoreDB, coll, verify); err != nil {
				Error.Printf("Restore failed: %v", err)
				os.Exit(1)
			}
			Info.Printf("Restore completed: DB=%s Collection=%s", restoreDB, coll)
			return
		}

		if err := RestoreDatabase(db, restoreDB, verify); err != nil {
			Error.Printf("Restore database failed: %v", err)
			os.Exit(1)
		}
	},
}

// ------------------ BULK-RESTORE COMMAND ------------------
var bulkRestoreCmd = &cobra.Command{
	Use:   "bulk-restore",
	Short: "Restore a collection across all databases in backup",
	Run: func(cmd *cobra.Command, args []string) {
		collection, _ := cmd.Flags().GetString("collection")
		verify, _ := cmd.Flags().GetBool("verify")
		sandbox, _ := cmd.Flags().GetBool("sandbox")

		if collection == "" {
			fmt.Println("You must provide --collection")
			os.Exit(1)
		}

		backupBase := AppConfig.BackupPath
		dbEntries, err := os.ReadDir(backupBase)
		if err != nil {
			Error.Printf("Failed to read backup path %s: %v", backupBase, err)
			os.Exit(1)
		}

		// CHANGED: thay vì tạo workerpool ở CLI, chỉ build restoreList
		var restoreList []string
		for _, dbEntry := range dbEntries {
			if !dbEntry.IsDir() {
				continue
			}
			dbName := dbEntry.Name()
			collFolder := filepath.Join(backupBase, dbName, collection, dbName)
			if info, err := os.Stat(collFolder); err == nil && info.IsDir() {
				bsonFile := filepath.Join(collFolder, collection+".bson.s2")
				if _, err := os.Stat(bsonFile); err == nil {
					restoreList = append(restoreList, bsonFile)
				}
			}
		}

		if len(restoreList) == 0 {
			Warn.Printf("No databases contain collection %s", collection)
			return
		}

		Info.Printf("Starting bulk restore for collection %s across %d databases (verify=%v sandbox=%v)",
			collection, len(restoreList), verify, sandbox)

		// CHANGED: gọi thẳng utils.BulkRestore (đã có worker pool + verify + cleanup)
		BulkRestore(restoreList, "", collection, verify)

		Info.Println("Bulk restore completed")
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
	restoreCmd.Flags().String("file", "", "Optional: Path to backup file or folder (default auto-detect in backup path)")
	restoreCmd.Flags().Bool("all", false, "Restore all databases")
	restoreCmd.Flags().Bool("sandbox", false, "Restore into sandbox database (DB_sandbox)")
	restoreCmd.Flags().Bool("verify", false, "Verify integrity before restore")

	// Bulk-restore flags
	bulkRestoreCmd.Flags().String("collection", "", "Collection name to restore across all databases (required)")
	bulkRestoreCmd.Flags().Bool("verify", false, "Verify BSON and metadata integrity before restore")
	bulkRestoreCmd.Flags().Bool("sandbox", false, "Restore into sandbox database (DB_sandbox)")

	// Add commands to root
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(bulkRestoreCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
