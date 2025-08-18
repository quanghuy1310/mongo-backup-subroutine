package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mongoClient *mongo.Client

func ConnectMongo(uri string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	// Ping để xác nhận kết nối thành công
	if err := client.Ping(ctx, nil); err != nil {
		return err
	}

	mongoClient = client
	fmt.Println("[INFO] Đã kết nối thành công tới MongoDB!")
	return nil
}

// Save backup status
func SaveBackupStatus(dbName, date, status, msg string) error {
	coll := mongoClient.Database("admin").Collection("backupStatus")
	_, err := coll.InsertOne(context.Background(), bson.M{
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
	coll := mongoClient.Database("admin").Collection("backupStatus")
	count, err := coll.CountDocuments(context.Background(), bson.M{
		"database": dbName,
		"date":     date,
		"status":   "success",
	})
	return count > 0, err
}

// Get list of all databases in YYYY_ProviderID format
func ListProviderDatabases() ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dbs, err := mongoClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	var filtered []string
	for _, db := range dbs {
		if len(db) >= 5 && db[4] == '_' { // simple YYYY_ProviderID check
			filtered = append(filtered, db)
		}
	}
	return filtered, nil
}
