# Secure Deployment Guide (Mumbai Server)

This guide is a step-by-step checklist to deploy this app on a Mumbai VPS with security in mind. It avoids code changes and focuses on safe ops.

## 1) Server Baseline
1. Use Ubuntu 22.04 LTS (or equivalent stable Linux).
2. Create a non-root user (e.g., `deploy`).
3. Enable automatic security updates.
4. Set correct timezone to IST.

Commands (run as root):

```bash
adduser deploy
usermod -aG sudo deploy
apt update && apt upgrade -y
apt install -y unattended-upgrades
sudo dpkg-reconfigure --priority=low unattended-upgrades
sudo timedatectl set-timezone Asia/Kolkata
```

## 2) SSH Hardening
1. Use SSH keys only (disable password auth).
2. Disable root login.
3. Change default SSH port only if required by your policy.

Edit `/etc/ssh/sshd_config`:

```text
PermitRootLogin no
PasswordAuthentication no
```

Restart SSH:

```bash
sudo systemctl restart ssh
```

## 3) Firewall Rules
Allow only required ports.

Example (UFW):

```bash
sudo ufw allow OpenSSH
sudo ufw allow 8000/tcp
sudo ufw enable
sudo ufw status
```

If you use a reverse proxy (recommended), only allow 80/443 externally and keep 8000 internal.

## 4) Install System Dependencies
```bash
sudo apt install -y python3 python3-venv python3-pip redis-server nginx
sudo systemctl enable redis-server
sudo systemctl start redis-server
```

## 5) App Setup
1. Copy project to `/home/deploy/algo_app_vash`.
2. Create virtualenv.
3. Install requirements.

```bash
cd /home/deploy
# upload or git clone
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 6) Environment Secrets
Create `.env` file and protect it.

Example keys used:
- `REDIS_URL`
- Zerodha `api_key`, access token stored in Redis at runtime

Ensure `.env` perms:

```bash
chmod 600 .env
```

## 7) Run Services with systemd (recommended)
Create systemd unit files for:
- FastAPI (`uvicorn main:app`)
- `kite_ws_worker.py`

Example directory:
- `/etc/systemd/system/algo-api.service`
- `/etc/systemd/system/algo-worker.service`

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable algo-api algo-worker
sudo systemctl start algo-api algo-worker
sudo systemctl status algo-api algo-worker
```

## 8) Reverse Proxy + TLS (optional but recommended)
Use Nginx + SSL (LetsEncrypt) so you don�t expose port 8000.

```bash
sudo apt install -y certbot python3-certbot-nginx
```

Configure Nginx to proxy to `127.0.0.1:8000`, then:

```bash
sudo certbot --nginx -d yourdomain.com
```

## 9) Daily Tasks
You have two options:

A) Use OS scheduler (recommended):
- Schedule `python fetch_nse_cash_instruments.py --user-id 1 --fetch-circuit` daily 09:00 IST.

B) In-app timer (already in `start_all.ps1`) only works if terminal is open.

## 10) Logs & Monitoring
- Rotate logs (systemd journalctl is okay for start)
- Set alert if worker dies

Example:

```bash
journalctl -u algo-api -f
journalctl -u algo-worker -f
```

## 11) Security Checklist
- No secrets in code
- `.env` not readable by others
- SSH keys only
- Firewall enabled
- Redis not exposed publicly
- Nginx + TLS if public

## 12) Post-Deploy Validation
1. Open dashboard and confirm WS live.
2. Check Redis connection.
3. Trigger small test alert.
4. Confirm orders placed correctly.

---

If you want, I can also provide:
- systemd unit templates
- Nginx config template
- monitoring scripts
