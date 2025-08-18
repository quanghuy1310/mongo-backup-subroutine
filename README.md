# Mongo Backup Subroutine

This project automates the backup of MongoDB databases using Go and mongodump.

## Features
- Connects to MongoDB and lists databases matching a specific format
- Backs up collections for each database using mongodump
- Supports retry logic and backup status tracking
- Loads configuration from a `.env` file

## Requirements
- Go 1.25+
- MongoDB Database Tools (mongodump must be in your PATH)
- Access to MongoDB server (local or via SSH tunnel)

## Usage
1. Clone the repository:
   ```sh
   git clone https://github.com/<your-username>/mongo-backup-subroutine.git
   cd mongo-backup-subroutine
   ```
2. Edit the `.env` file with your MongoDB URI and backup path.
3. Build the project:
   ```sh
   go build -o mongo_backup
   ```
4. Run the executable:
   ```sh
   ./mongo_backup
   ```

## SSH Tunnel Example
If your MongoDB server is remote, create an SSH tunnel:
```sh
ssh -L 27017:localhost:27017 user@remote-server
```
Set your `.env`:
```
MONGO_URI=mongodb://localhost:27017
```

## .env Example
```
MONGO_URI=mongodb://localhost:27017/?replicaSet=gsht-cluster
BACKUP_PATH=/mnt/mongo_backup
COMPRESSION=s2
RETRY_INTERVAL_MIN=5
MAX_RETRIES=5
```

## License
MIT
