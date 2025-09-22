package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Root command
var rootCmd = &cobra.Command{
	Use:   "mongobackup",
	Short: "MongoDB Backup Subroutine CLI",
	Long:  "MongoDB Backup Subroutine - manage MongoDB backup and restore operations",
}

// backupCmd: backup one collection
var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup a collection from a database",
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := cmd.Flags().GetString("db")
		coll, _ := cmd.Flags().GetString("collection")

		if db == "" || coll == "" {
			fmt.Println("You must provide --db and --collection")
			os.Exit(1)
		}

		// TODO: implement actual collection backup
		fmt.Printf("Starting backup for collection %s.%s ...\n", db, coll)
		// Example: RunCollectionBackup(db, coll)
	},
}

// restoreCmd: restore one collection
var restoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore a collection into a database",
	Run: func(cmd *cobra.Command, args []string) {
		db, _ := cmd.Flags().GetString("db")
		coll, _ := cmd.Flags().GetString("collection")
		file, _ := cmd.Flags().GetString("file")

		if db == "" || coll == "" || file == "" {
			fmt.Println("You must provide --db, --collection, and --file")
			os.Exit(1)
		}

		// TODO: implement actual collection restore
		fmt.Printf("Restoring collection %s.%s from file %s ...\n", db, coll, file)
		// Example: RunCollectionRestore(db, coll, file)
	},
}

// Execute CLI
func Execute() {
	// Flags for backup
	backupCmd.Flags().String("db", "", "Database name")
	backupCmd.Flags().String("collection", "", "Collection name")

	// Flags for restore
	restoreCmd.Flags().String("db", "", "Database name")
	restoreCmd.Flags().String("collection", "", "Collection name")
	restoreCmd.Flags().String("file", "", "Path to backup file (.bson or .archive)")

	// Add commands to root
	rootCmd.AddCommand(backupCmd)
	rootCmd.AddCommand(restoreCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
