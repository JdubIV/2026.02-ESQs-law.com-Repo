# =====================================================================================
# VS Code Multi-Device Setup Script for ESQs Legal System
# =====================================================================================

Write-Host "🔗 Setting up VS Code for Multi-Device Access..." -ForegroundColor Green
Write-Host "🏢 LAW Matrix v4.0 Bulletproof Enterprise Edition" -ForegroundColor Cyan
Write-Host ""

# Menu for setup options
Write-Host "Choose your setup option:" -ForegroundColor Yellow
Write-Host "1. 🌐 VS Code Tunnel (Remote Access - Personal Use)"
Write-Host "2. 🤝 Live Share Session (Real-time Collaboration)"
Write-Host "3. 📱 GitHub Codespaces (Cloud Development)"
Write-Host "4. ℹ️  Show current connections"
Write-Host "5. 🚪 Exit"
Write-Host ""

$choice = Read-Host "Enter your choice (1-5)"

switch ($choice) {
    "1" {
        Write-Host ""
        Write-Host "🌐 Setting up VS Code Tunnel..." -ForegroundColor Cyan
        Write-Host ""
        
        # Check if user is logged in
        Write-Host "🔐 Checking authentication status..." -ForegroundColor Yellow
        try {
            $authStatus = code tunnel user show 2>&1
            if ($authStatus -match "not signed in" -or $LASTEXITCODE -ne 0) {
                Write-Host "🔑 Please sign in to your Microsoft/GitHub account..." -ForegroundColor Yellow
                Write-Host "This will open a browser for authentication." -ForegroundColor Gray
                code tunnel user login
                
                if ($LASTEXITCODE -eq 0) {
                    Write-Host "✅ Authentication successful!" -ForegroundColor Green
                } else {
                    Write-Host "❌ Authentication failed. Please try again." -ForegroundColor Red
                    exit 1
                }
            } else {
                Write-Host "✅ Already authenticated!" -ForegroundColor Green
            }
        } catch {
            Write-Host "🔑 Setting up authentication..." -ForegroundColor Yellow
            code tunnel user login
        }
        
        Write-Host ""
        Write-Host "🚀 Creating tunnel for ESQs Legal System..." -ForegroundColor Cyan
        Write-Host "⚠️  Keep this window open to maintain the tunnel" -ForegroundColor Red
        Write-Host ""
        
        # Start the tunnel
        $tunnelName = "esqs-legal-boyack-$(Get-Date -Format 'MMdd')"
        Write-Host "🔗 Tunnel Name: $tunnelName" -ForegroundColor Green
        Write-Host "🌐 Access URL: https://vscode.dev/tunnel/$tunnelName" -ForegroundColor Green
        Write-Host ""
        
        code tunnel --accept-server-license-terms --name $tunnelName
    }
    
    "2" {
        Write-Host ""
        Write-Host "🤝 Starting Live Share Session..." -ForegroundColor Cyan
        Write-Host ""
        
        # Check if Live Share is installed
        $extensions = code --list-extensions
        if ($extensions -contains "ms-vsliveshare.vsliveshare") {
            Write-Host "✅ Live Share extension is installed!" -ForegroundColor Green
        } else {
            Write-Host "📥 Installing Live Share extension..." -ForegroundColor Yellow
            code --install-extension ms-vsliveshare.vsliveshare
            Write-Host "✅ Live Share installed!" -ForegroundColor Green
        }
        
        Write-Host ""
        Write-Host "🎯 Instructions to start Live Share:" -ForegroundColor Yellow
        Write-Host "1. Press Ctrl+Shift+P in VS Code"
        Write-Host "2. Type: Live Share: Start Collaboration Session"
        Write-Host "3. Copy the link that appears"
        Write-Host "4. Share with team members"
        Write-Host ""
        Write-Host "💡 Or use the Live Share button in the status bar!" -ForegroundColor Cyan
        
        # Open VS Code to current directory
        Write-Host "🚀 Opening VS Code..." -ForegroundColor Green
        code .
    }
    
    "3" {
        Write-Host ""
        Write-Host "📱 GitHub Codespaces Setup..." -ForegroundColor Cyan
        Write-Host ""
        Write-Host "🎯 To set up GitHub Codespaces:" -ForegroundColor Yellow
        Write-Host "1. Push your code to a GitHub repository"
        Write-Host "2. Go to your repo on GitHub.com"
        Write-Host "3. Click 'Code' → 'Codespaces' → 'Create codespace'"
        Write-Host "4. Access from any device at github.dev"
        Write-Host ""
        
        $createRepo = Read-Host "Do you want help creating a GitHub repo? (y/n)"
        if ($createRepo -eq "y" -or $createRepo -eq "Y") {
            Write-Host ""
            Write-Host "🔧 Setting up git repository..." -ForegroundColor Cyan
            
            # Initialize git if not already done
            if (!(Test-Path ".git")) {
                git init
                Write-Host "✅ Git repository initialized" -ForegroundColor Green
            }
            
            # Create .gitignore if it doesn't exist
            if (!(Test-Path ".gitignore")) {
                @"
node_modules/
.env
*.log
.vscode/settings.json
ngrok.log
"@ | Out-File -FilePath ".gitignore" -Encoding UTF8
                Write-Host "✅ .gitignore created" -ForegroundColor Green
            }
            
            Write-Host ""
            Write-Host "🎯 Next steps:" -ForegroundColor Yellow
            Write-Host "1. Create a new repository on GitHub.com"
            Write-Host "2. Run: git remote add origin https://github.com/yourusername/esqs-legal-system.git"
            Write-Host "3. Run: git add ."
            Write-Host "4. Run: git commit -m 'Initial commit - ESQs Legal System'"
            Write-Host "5. Run: git push -u origin main"
        }
    }
    
    "4" {
        Write-Host ""
        Write-Host "ℹ️  Current VS Code Connections:" -ForegroundColor Cyan
        Write-Host ""
        
        # Check tunnel status
        Write-Host "🌐 Tunnel Status:" -ForegroundColor Yellow
        try {
            code tunnel status
        } catch {
            Write-Host "❌ No active tunnels" -ForegroundColor Red
        }
        
        Write-Host ""
        Write-Host "🤝 Live Share Status:" -ForegroundColor Yellow
        Write-Host "Check the VS Code status bar for active sessions"
        Write-Host ""
        
        # Show network info
        Write-Host "🌍 Network Information:" -ForegroundColor Yellow
        Write-Host "Local Server: http://localhost:8080"
        if (Get-Process "lt" -ErrorAction SilentlyContinue) {
            Write-Host "LocalTunnel: Active (check terminal for URL)"
        } else {
            Write-Host "LocalTunnel: Not running"
        }
    }
    
    "5" {
        Write-Host "👋 Goodbye!" -ForegroundColor Green
        exit 0
    }
    
    default {
        Write-Host "❌ Invalid choice. Please run the script again." -ForegroundColor Red
        exit 1
    }
}

Write-Host ""
Write-Host "✅ Setup complete! Your ESQs Legal System is ready for multi-device access." -ForegroundColor Green
Read-Host "Press Enter to exit"