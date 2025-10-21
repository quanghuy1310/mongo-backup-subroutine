package main

import (
	"bufio"
	"fmt"
	"log"
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
	Long: `Backup command supports several use-cases:
- --all: backup all provider databases for the configured date (default yesterday) using GPS_YYYY_MM_DD collections
- --db <name>: backup the entire specified database (all collections)
- --collection <name>: scan all provider databases and backup the specified collection name if present
- --date YYYY-MM-DD: treat as collection name GPS_YYYY_MM_DD and behave like --collection
- --db + --date: backup only the given DB and the GPS_YYYY_MM_DD collection
Examples:
  mongobackup backup --all --date YYYY-MM-DD
  mongobackup backup --db YYYY_providerID
  mongobackup backup --collection myCollection
  mongobackup backup --date YYYY-MM-DD
`,
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := cmd.Flags().GetString("db")
		coll, _ := cmd.Flags().GetString("collection")
		allDBs, _ := cmd.Flags().GetBool("all")
		dateStr, _ := cmd.Flags().GetString("date")

		// Determine collection name from --date if provided
		var dateColl string
		if dateStr != "" {
			if _, err := time.Parse("2006-01-02", dateStr); err != nil {
				fmt.Println("Invalid date format, expected YYYY-MM-DD")
				os.Exit(1)
			}
			dateColl = fmt.Sprintf("GPS_%s", strings.ReplaceAll(dateStr, "-", "_"))
		}

		// Priority: --all overrides individual targets
		if allDBs {
			// when --all is used with --date, use that date; otherwise default yesterday
			var targetDate time.Time
			if dateColl != "" {
				// parse date back from dateStr
				targetDate, _ = time.Parse("2006-01-02", dateStr)
			} else {
				targetDate = time.Now().AddDate(0, 0, -1)
			}
			Info.Println("Starting backup for all databases ...")
			// CLI --all should be single-day and predictable; use RunFullBackupSingleDay
			RunFullBackupSingleDay(targetDate)
			return
		}

		// Case: both --db and --date -> backup the specific GPS collection in the db
		if db != "" && dateColl != "" {
			Info.Printf("Backing up DB=%s Collection=%s", db, dateColl)
			res := BackupCollectionSingle(db, dateColl)
			if res.Error != nil {
				if strings.HasPrefix(res.Error.Error(), "skipped:") {
					Warn.Printf("Backup skipped: %s.%s Reason=%s", db, dateColl, res.SkipReason)
				} else {
					Error.Printf("Backup failed: %s.%s Error=%v", db, dateColl, res.Error)
				}
			}
			return
		}

		// Case: only --db -> backup all collections in that DB
		if db != "" && coll == "" && dateColl == "" {
			Info.Printf("Backing up entire database %s ...", db)
			BackupEntireDatabase(db)
			return
		}

		// Case: only --collection (scan all DBs for that collection and backup when present)
		if coll != "" {
			Info.Printf("Scanning all databases for collection %s ...", coll)
			dbs, err := ListProviderDatabases()
			if err != nil {
				Error.Printf("Failed to list databases: %v", err)
				return
			}
			for _, d := range dbs {
				if CollectionExists(d, coll) {
					Info.Printf("Found %s in DB=%s, backing up...", coll, d)
					res := BackupCollectionSingle(d, coll)
					if res.Error != nil {
						if strings.HasPrefix(res.Error.Error(), "skipped:") {
							Warn.Printf("Skipped %s.%s: %s", d, coll, res.SkipReason)
						} else {
							Error.Printf("Failed to backup %s.%s: %v", d, coll, res.Error)
						}
					}
				}
			}
			return
		}

		// Case: only --date (treat as GPS_YYYY_MM_DD collection) -> same as --collection
		if dateColl != "" {
			Info.Printf("Scanning all databases for collection %s ...", dateColl)
			dbs, err := ListProviderDatabases()
			if err != nil {
				Error.Printf("Failed to list databases: %v", err)
				return
			}
			for _, d := range dbs {
				if CollectionExists(d, dateColl) {
					Info.Printf("Found %s in DB=%s, backing up...", dateColl, d)
					res := BackupCollectionSingle(d, dateColl)
					if res.Error != nil {
						if strings.HasPrefix(res.Error.Error(), "skipped:") {
							Warn.Printf("Skipped %s.%s: %s", d, dateColl, res.SkipReason)
						} else {
							Error.Printf("Failed to backup %s.%s: %v", d, dateColl, res.Error)
						}
					}
				}
			}
			return
		}

		// If we reach here, no valid combination was provided
		fmt.Println("You must provide one of: --all, --db, --collection, or --date")
		os.Exit(1)
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
			if !confirmPrompt(fmt.Sprintf("Proceed to restore %s.%s into %s? This will DROP existing collection if present.", db, coll, restoreDB)) {
				Info.Println("Restore cancelled by user")
				return
			}
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

		// Confirm because bulk restore is potentially destructive
		if !confirmPrompt(fmt.Sprintf("Proceed to bulk-restore collection %s across %d databases? This may DROP collections in target DBs.", collection, len(restoreList))) {
			Info.Println("Bulk restore cancelled by user")
			return
		}

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

	// Ensure Mongo connection is available for CLI commands that need it
	if mongoClient == nil {
		if err := ConnectMongo(AppConfig.MongoURI); err != nil {
			Error.Printf("Failed to connect MongoDB for CLI: %v", err)
			fmt.Println("Failed to connect MongoDB:", err)
			os.Exit(1)
		}
		defer DisconnectMongo()
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// confirmPrompt asks user Y/N for confirmation; returns true if user confirms
func confirmPrompt(prompt string) bool {
	fmt.Printf("%s [y/N]: ", prompt)
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read user input: %v", err)
		return false
	}
	resp := strings.TrimSpace(strings.ToLower(input))
	return resp == "y" || resp == "yes"
}
