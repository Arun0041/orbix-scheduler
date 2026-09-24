#!/bin/sh
# Orbix Demo Job Seeder for Linux / macOS
BASE="${1:-http://localhost:8081}"

echo "Seeding demo jobs to $BASE..."

curl -s -X POST "$BASE/api/jobs" -H "Content-Type: application/json" -d '{
  "name": "heartbeat",
  "cron": "*/10 * * * * *",
  "command": "echo \"heartbeat tick\" && sleep 2",
  "timeout_sec": 10,
  "max_retries": 3,
  "catch_up": "run_latest",
  "no_overlap": true,
  "enabled": true
}' > /dev/null

curl -s -X POST "$BASE/api/jobs" -H "Content-Type: application/json" -d '{
  "name": "cleanup-temp-data",
  "cron": "*/1 * * * *",
  "command": "echo \"cleaning up temporary files... done\"",
  "timeout_sec": 30,
  "max_retries": 2,
  "catch_up": "skip",
  "no_overlap": false,
  "enabled": true
}' > /dev/null

curl -s -X POST "$BASE/api/jobs" -H "Content-Type: application/json" -d '{
  "name": "database-backup",
  "cron": "0 0 * * *",
  "command": "echo \"simulating database backup...\" && sleep 5",
  "timeout_sec": 300,
  "max_retries": 3,
  "catch_up": "run_all",
  "no_overlap": true,
  "enabled": true
}' > /dev/null

echo "Seeding completed."
