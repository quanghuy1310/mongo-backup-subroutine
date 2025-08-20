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

// Connect to MongoDB
func ConnectMongo(uri string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	// Ping to confirm connection
	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	mongoClient = client
	logPrint("INFO", "Successfully connected to MongoDB")

	// Ensure indexes exist
	if err := EnsureIndexes(); err != nil {
		logPrint("ERROR", fmt.Sprintf("Failed to ensure indexes: %v", err))
	}

	return nil
}

// Disconnect from MongoDB
func DisconnectMongo() {
	if mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mongoClient.Disconnect(ctx); err != nil {
			logPrint("ERROR", fmt.Sprintf("Failed to disconnect MongoDB: %v", err))
		} else {
			logPrint("INFO", "MongoDB connection closed")
		}
	}
}

// EnsureIndexes creates useful indexes for backup collections
func EnsureIndexes() error {
	coll := mongoClient.Database("admin").Collection("backupStatus")
	_, err := coll.Indexes().CreateOne(context.Background(), mongo.IndexModel{
		Keys: bson.D{
			{Key: "database", Value: 1},
			{Key: "date", Value: 1},
			{Key: "status", Value: 1},
		},
	})
	return err
}

// Save backup status
func SaveBackupStatus(dbName, date, status, msg string) error {
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
	return err
}

// Check if backup was successful
func IsBackupDone(dbName, date string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	coll := mongoClient.Database("admin").Collection("backupStatus")
	count, err := coll.CountDocuments(ctx, bson.M{
		"database": dbName,
		"date":     date,
		"status":   "success",
	})
	return count > 0, err
}

// Get list of provider databases (pattern: YYYY_providerId)
func ListProviderDatabases() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbs, err := mongoClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	var filtered []string
	providerDbPattern := regexp.MustCompile(`^\d{4}_.+`)
	for _, db := range dbs {
		if providerDbPattern.MatchString(db) {
			filtered = append(filtered, db)
		}
	}
	return filtered, nil
}
