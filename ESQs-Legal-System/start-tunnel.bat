@echo off
REM =====================================================================================
REM LAW MATRIX v4.0 BULLETPROOF - SECURE TUNNEL STARTUP
REM =====================================================================================
echo 🚀 Starting LAW Matrix v4.0 Bulletproof Enterprise Edition with Secure Tunnel...
echo.

REM Start the server in background
echo 📡 Starting LAW Matrix Server on port 8080...
start /min "LAW Matrix Server" powershell -Command "cd '%~dp0'; npm start"

REM Wait a moment for server to start
timeout /t 5 /nobreak > nul

REM Start ngrok tunnel
echo 🌐 Creating secure tunnel...
echo 🔐 Basic authentication: esqs/legal2024
echo.
echo 📱 Your ESQs Legal System will be accessible from any device!
echo ⚠️  Keep this window open to maintain the tunnel
echo.

ngrok http --config ngrok.yml 8080

echo.
echo 🛑 Tunnel stopped. LAW Matrix Server may still be running.
pause