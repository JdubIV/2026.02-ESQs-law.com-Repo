# =====================================================================================
# LAW MATRIX v4.0 BULLETPROOF - SECURE TUNNEL SETUP GUIDE
# =====================================================================================

## Quick Start
1. Run `start-tunnel.bat` to start both server and tunnel
2. Copy the HTTPS URL provided by ngrok
3. Access your ESQs Legal System from any device using that URL

## Setup Instructions

### 1. Get ngrok Account (Recommended for persistent URLs)
- Go to https://ngrok.com and sign up for a free account
- Go to your dashboard and copy your authtoken
- Run: `ngrok config add-authtoken YOUR_AUTH_TOKEN`

### 2. Start the Tunnel
```bash
# Option 1: Use the batch file (easiest)
start-tunnel.bat

# Option 2: Manual start
npm start                    # Start server in one terminal
ngrok http 8080             # Start tunnel in another terminal
```

### 3. Security Features
- **Basic Authentication**: Username: `esqs` Password: `legal2024`
- **HTTPS Only**: All connections are encrypted
- **Temporary URLs**: URLs change each restart (upgrade to paid for permanent)

### 4. Accessing from Other Devices

Once the tunnel is running, you'll see output like:
```
Forwarding  https://abc123.ngrok.io -> http://localhost:8080
```

Use the HTTPS URL (https://abc123.ngrok.io) on any device:
- **Desktop**: Open in any browser
- **Mobile**: Open in mobile browser
- **Tablet**: Open in tablet browser
- **Other Computers**: Access via the same URL

### 5. Advanced Configuration

#### Custom Subdomain (Paid Plan)
Edit `ngrok.yml` and add:
```yaml
tunnels:
  esqs-legal:
    proto: http
    addr: 8080
    subdomain: "your-custom-name"
```

#### Remove Basic Auth (if desired)
Edit `ngrok.yml` and remove the `auth` line.

### 6. Alternative Tunnel Services

#### Cloudflare Tunnel (Free, Permanent URLs)
```bash
npm install -g cloudflared
cloudflared tunnel --url localhost:8080
```

#### VS Code Tunnel (If using VS Code)
```bash
code tunnel --accept-server-license-terms
```

#### LocalTunnel (Simple, No signup)
```bash
npm install -g localtunnel
lt --port 8080 --subdomain esqs-legal
```

### 7. Security Recommendations
- Change the default auth credentials in `ngrok.yml`
- Only share the tunnel URL with authorized users
- Stop the tunnel when not needed
- Consider upgrading to ngrok paid plan for better security features

### 8. Troubleshooting
- If tunnel fails to start, ensure server is running on port 8080
- If authentication fails, check credentials in `ngrok.yml`
- If URL is not accessible, check firewall settings

### 9. Production Considerations
For production use, consider:
- Proper domain with SSL certificate
- VPN access instead of public tunnel
- Cloud deployment (Azure, AWS, etc.)
- Professional tunnel service with SLA

## Support
For issues with the LAW Matrix system, check the server logs.
For tunnel issues, check `ngrok.log` file.