package main

import (
	"fmt"
	"time"
)

func main() {
	LoadConfig()

	err := ConnectMongo(AppConfig.MongoURI)
	if err != nil {
		panic(err)
	}

	yesterday := time.Now().AddDate(0, 0, -1)
	dbs, err := ListProviderDatabases()
	if err != nil {
		panic(err)
	}

	for _, db := range dbs {
		fmt.Println("Backup DB:", db)
		BackupWithRetry(db, yesterday)
	}
}
