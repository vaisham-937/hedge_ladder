param(
  [string]$UserId = "1",
  [int]$Port = 8000,
  [string]$StartAt = "09:00"
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# Reduce ladder logs to colored-only lines (plus errors)
$env:LADDER_LOG_COLOR_ONLY = "1"

# Optional: wait until StartAt (HH:mm) in IST before starting
try {
  $tz = [TimeZoneInfo]::FindSystemTimeZoneById("India Standard Time")
  $nowIst = [TimeZoneInfo]::ConvertTime((Get-Date), $tz)
  $startTime = [DateTime]::ParseExact($StartAt, "HH:mm", $null)
  $targetIst = Get-Date -Date $nowIst.Date -Hour $startTime.Hour -Minute $startTime.Minute -Second 0
  if ($targetIst -le $nowIst) { $targetIst = $targetIst.AddDays(1) }
  $wait = $targetIst - $nowIst
  if ($wait.TotalSeconds -gt 0) {
    Write-Host ("Waiting until IST {0:yyyy-MM-dd HH:mm}..." -f $targetIst)
    Start-Sleep -Seconds [int]$wait.TotalSeconds
  }
} catch {
  Write-Host "Invalid StartAt time format. Use HH:mm (e.g., 09:00). Skipping wait."
}


Write-Host "Starting FastAPI server (same terminal)..."
$p1 = Start-Process -NoNewWindow -PassThru -FilePath "python" -ArgumentList @("-m","uvicorn","main:app","--host","127.0.0.1","--port",$Port)

Start-Sleep -Seconds 2

Write-Host "Starting Kite WS worker (same terminal)..."
$p2 = Start-Process -NoNewWindow -PassThru -FilePath "python" -ArgumentList @("kite_ws_worker.py","--user-id",$UserId)

Write-Host "Starting circuit/open watcher (same terminal)..."
$p3 = Start-Process -NoNewWindow -PassThru -FilePath "python" -ArgumentList @("fetch_nse_cash_instruments.py","--user-id",$UserId,"--watch")

Write-Host "All processes started in this terminal. Logs will be interleaved."
Wait-Process -Id @($p1.Id, $p2.Id, $p3.Id)
