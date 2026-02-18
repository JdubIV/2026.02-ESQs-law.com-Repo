# =====================================================================================
# LAW MATRIX v4.0 BULLETPROOF - SECURE TUNNEL STARTUP (PowerShell)
# =====================================================================================

Write-Host "🚀 Starting LAW Matrix v4.0 Bulletproof Enterprise Edition with Secure Tunnel..." -ForegroundColor Green
Write-Host ""

# Check if ngrok is installed
try {
    $ngrokVersion = ngrok version
    Write-Host "✅ ngrok is installed: $ngrokVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ ngrok not found. Installing..." -ForegroundColor Red
    npm install -g ngrok
}

# Start the server in background
Write-Host "📡 Starting LAW Matrix Server on port 8080..." -ForegroundColor Cyan
$serverJob = Start-Job -ScriptBlock { 
    Set-Location $using:PWD
    npm start 
}

# Wait for server to start
Write-Host "⏳ Waiting for server to initialize..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Check if server is running
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 5 -UseBasicParsing
    Write-Host "✅ LAW Matrix Server is running!" -ForegroundColor Green
} catch {
    Write-Host "⚠️ Server may still be starting..." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "🌐 Creating secure tunnel..." -ForegroundColor Cyan
Write-Host "🔐 Basic authentication: esqs/legal2024" -ForegroundColor Yellow
Write-Host ""
Write-Host "📱 Your ESQs Legal System will be accessible from any device!" -ForegroundColor Green
Write-Host "⚠️  Keep this window open to maintain the tunnel" -ForegroundColor Red
Write-Host ""

# Start ngrok tunnel
try {
    if (Test-Path "ngrok.yml") {
        ngrok http --config ngrok.yml 8080
    } else {
        ngrok http 8080
    }
} catch {
    Write-Host "❌ Failed to start tunnel. Error: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host ""
Write-Host "🛑 Tunnel stopped. Cleaning up..." -ForegroundColor Yellow

# Stop the server job
if ($serverJob) {
    Stop-Job $serverJob
    Remove-Job $serverJob
    Write-Host "🛑 LAW Matrix Server stopped." -ForegroundColor Yellow
}

Write-Host "✅ Cleanup complete." -ForegroundColor Green
Read-Host "Press Enter to exit"