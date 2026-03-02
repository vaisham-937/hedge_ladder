param(
  [string]$UserId = "1",
  [int]$Port = 8000
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# Reduce ladder logs to colored-only lines (plus errors)
$env:LADDER_LOG_COLOR_ONLY = "1"

Write-Host "Starting FastAPI server (same terminal)..."
$p1 = Start-Process -NoNewWindow -PassThru -FilePath "python" -ArgumentList @("-m","uvicorn","main:app","--host","127.0.0.1","--port",$Port)

Start-Sleep -Seconds 3

Write-Host "Starting Kite WS worker (same terminal)..."
$p2 = Start-Process -NoNewWindow -PassThru -FilePath "python" -ArgumentList @("kite_ws_worker.py","--user-id",$UserId)

Write-Host "All processes started in this terminal. Logs will be interleaved."
Wait-Process -Id @($p1.Id, $p2.Id)
