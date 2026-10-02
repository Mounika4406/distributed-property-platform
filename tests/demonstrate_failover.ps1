Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "      NGINX Regional Failover Demonstration Script       " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$baseUrl = "http://localhost:8080"

Write-Host "[1/6] Ensuring all Docker services are running..." -ForegroundColor Yellow
docker compose up -d

Write-Host "Waiting for services to become healthy..."
$healthy = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resUs = Invoke-WebRequest -Uri "$baseUrl/us/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
        $resEu = Invoke-WebRequest -Uri "$baseUrl/eu/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction SilentlyContinue
        if ($resUs.StatusCode -eq 200 -and $resEu.StatusCode -eq 200) {
            $healthy = $true
            break
        }
    } catch {}
    Start-Sleep -Seconds 3
}

if (-not $healthy) {
    Write-Host "ERROR: Backends not healthy within timeout." -ForegroundColor Red
    docker compose ps
    exit 1
}

Write-Host "`n[2/6] Sending GET request to /us/health (Normal Operation)..." -ForegroundColor Yellow
$normal = Invoke-WebRequest -Uri "$baseUrl/us/health" -UseBasicParsing
Write-Host "Response Status: $($normal.StatusCode)"
Write-Host "Response Body: $($normal.Content)"

Write-Host "`n[3/6] Inspecting backend_us logs..." -ForegroundColor Yellow
docker logs backend_us --tail 4

Write-Host "`n[4/6] Simulating US region failure by stopping backend_us..." -ForegroundColor Yellow
docker stop backend_us
Start-Sleep -Seconds 2

Write-Host "`n[5/6] Sending GET request to /us/health while backend_us is DOWN..." -ForegroundColor Yellow
$failover = Invoke-WebRequest -Uri "$baseUrl/us/health" -UseBasicParsing
Write-Host "Response Status: $($failover.StatusCode)"
Write-Host "Response Body: $($failover.Content)"

if ($failover.StatusCode -ne 200) {
    Write-Host "ERROR: Expected 200 OK during failover, got $($failover.StatusCode)" -ForegroundColor Red
    docker start backend_us
    exit 1
}

Write-Host "`n[6/6] Inspecting backend_eu logs to verify EU handled the /us/ traffic..." -ForegroundColor Yellow
docker logs backend_eu --tail 6

Write-Host "`nRestarting backend_us container to restore cluster topology..." -ForegroundColor Yellow
docker start backend_us
Start-Sleep -Seconds 3

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host " SUCCESS: NGINX successfully routed to backend_eu! " -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
