# Orbix one-shot starter.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location (Join-Path $root 'deploy')
Write-Host '[1/5] Starting Docker stack (first build takes a few minutes)...'
docker compose up -d --build
if ($LASTEXITCODE -ne 0) { throw 'docker compose failed. Start Docker Desktop and retry.' }
Write-Host '[2/5] Waiting for node1 to become healthy...'
$ok = $false
for ($i = 0; $i -lt 40 -and -not $ok; $i++) {
  try {
    $h = Invoke-RestMethod 'http://localhost:8081/healthz' -TimeoutSec 3
    if ($h.status -eq 'ok') { $ok = $true }
    else { Start-Sleep 3 }
  } catch { Start-Sleep 3 }
}
if (-not $ok) { throw 'node1 never became healthy. Diagnose: docker compose logs node1 etcd' }
Write-Host '[3/5] Seeding demo jobs (skips existing)...'
try {
  powershell -ExecutionPolicy Bypass -File (Join-Path $root 'scripts/seed.ps1') -Base 'http://localhost:8081'
  if ($LASTEXITCODE -ne 0) { Write-Warning 'seed script exited nonzero - continuing, the cluster is up' }
} catch {
  Write-Warning ("seed step failed (" + $_.Exception.Message + ") - continuing, the cluster is up")
}
Write-Host '[4/5] Cluster status:'
docker compose ps
Write-Host '[5/5] Opening console...'
Start-Process 'http://localhost:8081'
Write-Host 'Done. Consoles on 8081, 8082, 8083. Stop with: docker compose down'