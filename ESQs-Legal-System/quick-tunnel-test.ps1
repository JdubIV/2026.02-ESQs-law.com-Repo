# =====================================================================================
# LAW MATRIX v4.0 BULLETPROOF - QUICK TUNNEL TEST
# =====================================================================================

Write-Host "🚀 LAW Matrix v4.0 - Quick Tunnel Test" -ForegroundColor Green
Write-Host ""

# Start the server in background
Write-Host "📡 Starting LAW Matrix Server..." -ForegroundColor Cyan
$serverProcess = Start-Process -FilePath "node" -ArgumentList "server.js" -PassThru -WindowStyle Hidden

# Wait for server startup
Write-Host "⏳ Waiting for server to start..." -ForegroundColor Yellow
Start-Sleep -Seconds 8

# Test if server is responding
Write-Host "🔍 Testing server connection..." -ForegroundColor Cyan
try {
    $null = Invoke-WebRequest -Uri "http://localhost:8080" -TimeoutSec 10 -UseBasicParsing -ErrorAction Stop
    Write-Host "✅ LAW Matrix Server is running on port 8080!" -ForegroundColor Green
    
    Write-Host ""
    Write-Host "🌐 Creating secure tunnel with LocalTunnel..." -ForegroundColor Cyan
    Write-Host "📱 You'll get a public URL to access from any device!" -ForegroundColor Green
    Write-Host "⚠️  Press Ctrl+C to stop the tunnel and server" -ForegroundColor Yellow
    Write-Host ""
    
    # Create tunnel
    lt --port 8080 --subdomain esqs-legal-system
    
} catch {
    Write-Host "❌ Server not responding. Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host "💡 Try running 'npm start' manually first" -ForegroundColor Yellow
} finally {
    # Cleanup
    if ($serverProcess -and !$serverProcess.HasExited) {
        Write-Host ""
        Write-Host "🛑 Stopping LAW Matrix Server..." -ForegroundColor Yellow
        Stop-Process -Id $serverProcess.Id -Force -ErrorAction SilentlyContinue
        Write-Host "✅ Server stopped." -ForegroundColor Green
    }
}

Read-Host "Press Enter to exit"