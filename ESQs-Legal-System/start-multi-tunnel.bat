@echo off
REM =====================================================================================
REM LAW MATRIX v4.0 BULLETPROOF - MULTI-TUNNEL STARTUP
REM =====================================================================================
echo 🚀 LAW Matrix v4.0 Bulletproof - Tunnel Setup Options
echo.

:MENU
echo Select your tunnel option:
echo 1. LocalTunnel (Simple, No signup required)
echo 2. ngrok (Advanced, requires account for best features)
echo 3. Start server only (no tunnel)
echo 4. Exit
echo.
set /p choice="Enter your choice (1-4): "

if "%choice%"=="1" goto LOCALTUNNEL
if "%choice%"=="2" goto NGROK
if "%choice%"=="3" goto SERVERONLY
if "%choice%"=="4" exit
goto MENU

:LOCALTUNNEL
echo.
echo 📡 Starting LAW Matrix Server...
start /min "LAW Matrix Server" powershell -Command "cd '%~dp0'; npm start"
timeout /t 5 /nobreak > nul

echo 🌐 Creating LocalTunnel...
echo 📱 Your tunnel URL will be shown below
echo ⚠️  Keep this window open to maintain the tunnel
echo.
lt --port 8080 --subdomain esqs-legal-system
goto END

:NGROK
echo.
echo 📡 Starting LAW Matrix Server...
start /min "LAW Matrix Server" powershell -Command "cd '%~dp0'; npm start"
timeout /t 5 /nobreak > nul

echo 🌐 Creating ngrok tunnel...
echo 🔐 Basic authentication: esqs/legal2024 (if configured)
echo ⚠️  Keep this window open to maintain the tunnel
echo.
if exist ngrok.yml (
    ngrok http --config ngrok.yml 8080
) else (
    ngrok http 8080
)
goto END

:SERVERONLY
echo.
echo 📡 Starting LAW Matrix Server only (localhost access)...
echo 🌐 Access at: http://localhost:8080
npm start
goto END

:END
echo.
echo 🛑 Tunnel/Server stopped.
pause