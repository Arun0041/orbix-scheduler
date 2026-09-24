# Orbix Demo Job Seeder for Windows PowerShell
param (
    [string]$Base = 'http://localhost:8081'
)

$ErrorActionPreference = 'Stop'

Write-Host "Seeding demo jobs to $Base..."

$jobs = @(
    @{
        name = "heartbeat"
        cron = "*/10 * * * * *"
        command = "echo 'heartbeat tick' && sleep 2"
        timeout_sec = 10
        max_retries = 3
        catch_up = "run_latest"
        no_overlap = $true
        enabled = $true
    },
    @{
        name = "cleanup-temp-data"
        cron = "*/1 * * * *"
        command = "echo 'cleaning up temporary files... done'"
        timeout_sec = 30
        max_retries = 2
        catch_up = "skip"
        no_overlap = $false
        enabled = $true
    },
    @{
        name = "database-backup"
        cron = "0 0 * * *"
        command = "echo 'simulating database backup...' && sleep 5"
        timeout_sec = 300
        max_retries = 3
        catch_up = "run_all"
        no_overlap = $true
        enabled = $true
    }
)

foreach ($j in $jobs) {
    try {
        $json = $j | ConvertTo-Json -Depth 4
        $res = Invoke-RestMethod -Uri "$Base/api/jobs" -Method Post -Body $json -ContentType 'application/json'
        Write-Host "Seeded job: $($j.name)"
    } catch {
        Write-Host "Skipped or existing job: $($j.name)"
    }
}

Write-Host "Seeding completed."
