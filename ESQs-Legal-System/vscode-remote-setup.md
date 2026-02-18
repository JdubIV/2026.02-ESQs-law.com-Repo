# 🔗 VS Code Multi-Device Setup for ESQs Legal System

## 🎯 What This Enables

With VS Code remote access, you can:
- **Edit the same files** from multiple devices
- **Share your development environment** with team members
- **Access your full VS Code setup** (extensions, settings, terminal) remotely
- **Collaborate in real-time** on legal document automation
- **Debug and run code** from any device

## 🚀 Option 1: VS Code Tunnels (Personal Remote Access)

### Step 1: Authentication
First, sign in to your Microsoft/GitHub account:

```bash
# In VS Code terminal, run:
code tunnel user login
```

This will open a browser for you to authenticate.

### Step 2: Create Named Tunnel
```bash
# Create a persistent tunnel with a custom name
code tunnel --accept-server-license-terms --name "esqs-legal-boyack"
```

### Step 3: Access from Other Devices
1. **On any device**, go to: `https://vscode.dev/tunnel/esqs-legal-boyack`
2. **Or install VS Code** and use: `code --remote tunnel --name esqs-legal-boyack`
3. **Mobile**: Use GitHub Codespaces app or browser

---

## 🤝 Option 2: VS Code Live Share (Real-Time Collaboration)

### Step 1: Install Live Share Extension
```bash
# Install the Live Share extension
code --install-extension ms-vsliveshare.vsliveshare
```

### Step 2: Start Collaboration Session
1. **Press `Ctrl+Shift+P`** (Command Palette)
2. **Type**: "Live Share: Start Collaboration Session"
3. **Copy the link** that appears
4. **Share link** with team members

### Step 3: Join from Other Devices
- **Team members** click the shared link
- **Opens in VS Code** or browser
- **Real-time editing** with multiple cursors

---

## 🛠️ Setup Scripts for Easy Management

I'll create automated scripts for you: