@echo off
REM =====================================================================================
REM VS Code Multi-Device Access for ESQs Legal System
REM =====================================================================================
echo 🔗 VS Code Multi-Device Setup for ESQs Legal System
echo 🏢 LAW Matrix v4.0 Bulletproof Enterprise Edition
echo.

:MENU
echo Choose your option:
echo 1. 🌐 Start VS Code Tunnel (Remote Access)
echo 2. 🤝 Start Live Share (Real-time Collaboration) 
echo 3. 📋 Show Instructions Only
echo 4. 🚪 Exit
echo.
set /p choice="Enter your choice (1-4): "

if "%choice%"=="1" goto TUNNEL
if "%choice%"=="2" goto LIVESHARE
if "%choice%"=="3" goto INSTRUCTIONS
if "%choice%"=="4" exit
goto MENU

:TUNNEL
echo.
echo 🌐 Starting VS Code Tunnel...
echo 🔑 You may need to authenticate in your browser
echo ⚠️  Keep this window open to maintain the tunnel
echo.
powershell -ExecutionPolicy Bypass -File setup-vscode-remote.ps1
goto END

:LIVESHARE
echo.
echo 🤝 Setting up Live Share...
echo 📥 Installing Live Share extension (if needed)...
code --install-extension ms-vsliveshare.vsliveshare
echo.
echo ✅ Live Share ready!
echo 🎯 To start collaboration:
echo    1. Press Ctrl+Shift+P in VS Code
echo    2. Type: Live Share: Start Collaboration Session
echo    3. Share the link with team members
echo.
echo 🚀 Opening VS Code...
code .
goto END

:INSTRUCTIONS
echo.
echo 📋 VS Code Multi-Device Access Instructions
echo =============================================
echo.
echo 🌐 VS Code Tunnel (Personal Remote Access):
echo    • Run this script and choose option 1
echo    • Get a URL like: https://vscode.dev/tunnel/your-tunnel-name
echo    • Access your full VS Code from any device
echo    • Requires Microsoft/GitHub account
echo.
echo 🤝 Live Share (Real-time Collaboration):
echo    • Run this script and choose option 2
echo    • Start a session in VS Code (Ctrl+Shift+P)
echo    • Share the link with team members
echo    • Everyone can edit simultaneously
echo.
echo 📱 Access Options:
echo    • Browser: vscode.dev (works on tablets/phones)
echo    • Desktop: Install VS Code + connect to tunnel
echo    • Mobile: GitHub Mobile app or browser
echo.
echo 🔒 Security:
echo    • Tunnels require authentication
echo    • Live Share sessions are encrypted
echo    • You control who has access
echo.
pause
goto MENU

:END
echo.
echo ✅ VS Code setup complete!
echo 💡 Check vscode-remote-setup.md for detailed instructions
pause