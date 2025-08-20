package main

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoClient *mongo.Client

// ConnectMongo initializes a new MongoDB client with timeout
func ConnectMongo(uri string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	mongoClient = client
	Info.Println("MongoDB connected successfully")

	if err := EnsureIndexes(); err != nil {
		Error.Printf("Failed to ensure indexes: %v", err)
	}

	return nil
}

// DisconnectMongo closes MongoDB connection
func DisconnectMongo() {
	if mongoClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := mongoClient.Disconnect(ctx); err != nil {
		Error.Printf("Failed to disconnect MongoDB: %v", err)
	} else {
		Info.Println("MongoDB connection closed")
	}
}

// GetMongoClient returns the global mongoClient safely
func GetMongoClient() *mongo.Client {
	return mongoClient
}

// EnsureIndexes creates indexes for backup collections
func EnsureIndexes() error {
	if mongoClient == nil {
		return fmt.Errorf("mongoClient is nil")
	}
	coll := mongoClient.Database("admin").Collection("backupStatus")
	_, err := coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "database", Value: 1},
			{Key: "date", Value: 1},
			{Key: "status", Value: 1},
		},
	})
	if err == nil {
		Info.Println("Indexes ensured on backupStatus collection")
	}
	return err
}

// SaveBackupStatus inserts backup status document
func SaveBackupStatus(dbName, date, status, msg string) error {
	if mongoClient == nil {
		return fmt.Errorf("mongoClient is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	coll := mongoClient.Database("admin").Collection("backupStatus")
	_, err := coll.InsertOne(ctx, bson.M{
		"database":  dbName,
		"date":      date,
		"status":    status,
		"message":   msg,
		"timestamp": time.Now(),
	})
	if err != nil {
		Error.Printf("Failed to save backup status for %s (%s): %v", dbName, date, err)
	} else {
		Info.Printf("Backup status saved: %s (%s) -> %s", dbName, date, status)
	}
	return err
}

// IsBackupDone checks if backup for db+date succeeded
func IsBackupDone(dbName, date string) (bool, error) {
	if mongoClient == nil {
		return false, fmt.Errorf("mongoClient is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	coll := mongoClient.Database("admin").Collection("backupStatus")
	count, err := coll.CountDocuments(ctx, bson.M{
		"database": dbName,
		"date":     date,
		"status":   "success",
	})
	if err != nil {
		Error.Printf("Failed to check backup done for %s (%s): %v", dbName, date, err)
	}
	return count > 0, err
}

// ListProviderDatabases returns databases matching YYYY_providerId
func ListProviderDatabases() ([]string, error) {
	if mongoClient == nil {
		return nil, fmt.Errorf("mongoClient is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbs, err := mongoClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	var filtered []string
	providerDbPattern := regexp.MustCompile(`^\d{4}_.+`)
	for _, db := range dbs {
		if providerDbPattern.MatchString(db) {
			filtered = append(filtered, db)
		}
	}

	Info.Printf("Found %d provider databases for backup", len(filtered))
	return filtered, nil
}
