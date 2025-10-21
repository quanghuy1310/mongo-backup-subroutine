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
MONGO_URI=mongodb://localhost:27017/?
BACKUP_PATH=/mnt/mongo_backup
COMPRESSION=s2
RETRY_INTERVAL_MIN=5
MAX_RETRIES=5

# How many recent GPS daily collections to backup (1..3). MongoDB keeps last 3 days of GPS data.
# Use BACKUP_DAYS_INTERVAL=1 to backup only the most recent day (default).
# Use BACKUP_DAYS_INTERVAL=2 to backup last 2 days, or 3 for last 3 days.
BACKUP_DAYS_INTERVAL=1

## Verbose logging
You can control informational logging with the `VERBOSE` env var. When `VERBOSE=1` or `VERBOSE=true` the application will print Info logs to stdout and the log file. When false (default) only warnings and errors are printed to reduce noise.

Note about CLI `--all` behavior: the CLI `mongobackup backup --all` is deliberately single-day (default yesterday) and will not automatically expand to multiple days even if `BACKUP_DAYS_INTERVAL` is set. The `BACKUP_DAYS_INTERVAL` config applies to the scheduled/daemon runs. This keeps CLI invocations predictable.

## Running long commands over SSH

When you run long `mongobackup backup` or `restore` commands over an SSH session they may be terminated when your SSH session disconnects. Options to avoid this:

- Use tmux or screen: start a tmux session, run the command inside it, detach (session persists on server).
- Use nohup: `nohup mongobackup backup --db ... > /var/log/mongo_backup_manual.log 2>&1 &` — process continues after logout.
- Use systemd-run to run a one-shot transient service: `systemd-run --unit=mongobackup-run --remain-after-exit /opt/mongo-backup-subroutine/mongo_backup backup --db ...`
- Best: install as a systemd service (example in `contrib/mongo-backup.service`) and trigger via `systemctl start mongobackup-run` or use a dedicated timer for scheduled runs.

I recommend using `tmux` for ad-hoc interactive work and systemd for production scheduled tasks.
```

## License
MIT
