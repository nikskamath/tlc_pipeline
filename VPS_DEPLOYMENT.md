# Running TLC Pipeline on a VPS/Linux Server (No Docker Desktop)

Guide to deploy Airflow + n8n on a cheap Linux VPS instead of Docker Desktop.

---

## **Why VPS Instead of Docker Desktop?**

| Aspect | Docker Desktop | VPS |
|--------|---|---|
| **Cost** | Free (but resource-heavy) | $5–20/month |
| **Always-on** | Requires your machine running | Runs 24/7 independently |
| **Remote access** | Only on local machine | Access from anywhere |
| **CPU/RAM** | Competes with your machine | Dedicated resources |
| **Best for** | Local dev/testing | Production pipelines |

---

## **Option 1: DigitalOcean Droplet (Recommended for Beginners)**

### **Step 1: Create a Droplet**

1. Go to https://digitalocean.com
2. Click **Create → Droplets**
3. Choose:
   - **OS**: Ubuntu 22.04 LTS
   - **Plan**: Basic, $6/month (2GB RAM, 2 vCPU, 50GB SSD)
   - **Region**: Choose closest to you
   - **Auth**: SSH key (generate one)
4. Click **Create Droplet**
5. Note the IP address (e.g., `192.168.1.100`)

### **Step 2: SSH into the Server**

```bash
# On your local machine
ssh root@192.168.1.100

# Or if using private key
ssh -i ~/.ssh/id_rsa root@192.168.1.100
```

### **Step 3: Install Docker & Docker Compose**

```bash
# Update packages
apt update && apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
chmod +x /usr/local/bin/docker-compose

# Verify
docker --version
docker-compose --version
```

### **Step 4: Clone Your TLC Pipeline Repo**

```bash
# Install git
apt install git -y

# Clone the repo
cd /root
git clone https://github.com/nikskamath/tlc_pipeline.git
cd tlc_pipeline/snowflake

# Create .env file
cp .env.example .env

# Edit .env with your Snowflake + Gemini credentials
nano .env
# (Press Ctrl+X, Y to save)
```

### **Step 5: Start Services**

```bash
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f airflow-scheduler
```

### **Step 6: Access from Your Machine**

Add this to your local `/etc/hosts` (Mac/Linux) or `C:\Windows\System32\drivers\etc\hosts` (Windows):

```
192.168.1.100 tlc-pipeline.local
```

Then open in browser:
- **Airflow**: http://tlc-pipeline.local:8080
- **n8n**: http://tlc-pipeline.local:5678

---

## **Option 2: AWS EC2 (More Powerful)**

### **Step 1: Launch EC2 Instance**

```bash
# Via AWS Console or CLI
aws ec2 run-instances \
  --image-id ami-0c55b159cbfafe1f0 \  # Ubuntu 22.04 in us-east-1
  --instance-type t3.small \
  --key-name your-key-pair \
  --security-groups allow-ssh-8080-5678

# Get the public IP
aws ec2 describe-instances --instance-ids i-xxxxx \
  --query 'Reservations[0].Instances[0].PublicIpAddress'
```

### **Step 2: SSH & Setup**

(Same as DigitalOcean Steps 2–6 above, just use AWS public IP)

---

## **Option 3: Local Linux/Mac (Systemd Services)**

If you want to run natively (no Docker) on your always-on machine:

### **Step 1: Install Airflow & Dependencies**

```bash
# Create virtual environment
python3 -m venv ~/tlc-venv
source ~/tlc-venv/bin/activate

# Install Airflow
pip install apache-airflow==2.6.3 dbt-snowflake snowflake-connector-python

# Install n8n (requires Node.js 18+)
npm install -g n8n
```

### **Step 2: Initialize Airflow**

```bash
export AIRFLOW_HOME=~/tlc-airflow
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com
```

### **Step 3: Create Systemd Services**

**Airflow Scheduler** (`/etc/systemd/system/airflow-scheduler.service`):

```ini
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
Type=simple
User=your-username
Environment="AIRFLOW_HOME=/home/your-username/tlc-airflow"
ExecStart=/home/your-username/tlc-venv/bin/airflow scheduler
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

**Airflow Webserver** (`/etc/systemd/system/airflow-webserver.service`):

```ini
[Unit]
Description=Airflow Webserver
After=network.target

[Service]
Type=simple
User=your-username
Environment="AIRFLOW_HOME=/home/your-username/tlc-airflow"
ExecStart=/home/your-username/tlc-venv/bin/airflow webserver --port 8080
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

### **Step 4: Enable & Start**

```bash
sudo systemctl daemon-reload
sudo systemctl enable airflow-scheduler airflow-webserver
sudo systemctl start airflow-scheduler airflow-webserver

# Check status
sudo systemctl status airflow-scheduler
sudo systemctl status airflow-webserver
```

---

## **Option 4: Cloud Composer (Google Cloud)**

Most managed option (but pricier ~$100+/month):

```bash
gcloud composer environments create tlc-pipeline \
  --location us-central1 \
  --python-version 3 \
  --machine-type n1-standard-4

# Copy DAGs
gcloud composer environments storage dags import \
  --environment=tlc-pipeline \
  --location=us-central1 \
  --source=/path/to/dags/*

# View UI
gcloud composer environments describe tlc-pipeline \
  --location us-central1 \
  --format="value(config.airflowUri)"
```

---

## **Monitoring & Logging**

### **View Logs on VPS**

```bash
# Airflow logs
docker-compose logs -f airflow-scheduler

# n8n logs
docker-compose logs -f n8n

# Follow specific DAG
docker-compose exec airflow-scheduler tail -f /opt/airflow/logs/tlc_snowflake_pipeline/*.log
```

### **Set Up Log Streaming to CloudWatch (AWS)**

```bash
# Install CloudWatch agent on droplet
wget https://s3.amazonaws.com/amazoncloudwatch-agent/ubuntu/amd64/latest/amazon-cloudwatch-agent.deb
dpkg -i amazon-cloudwatch-agent.deb

# Configure & start
/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config \
  -m ec2 \
  -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/cloudwatch-config.json
```

---

## **Auto-Backup Snowflake Data**

Add a daily cron job to backup Snowflake exports:

```bash
# Edit crontab
crontab -e

# Add this line (runs daily at 2 AM UTC)
0 2 * * * /root/tlc_pipeline/snowflake/scripts/backup_snowflake.sh
```

Create `backup_snowflake.sh`:

```bash
#!/bin/bash

export SNOWSQL_PWD=$SF_PASSWORD

# Export Snowflake data
snowsql -a $SF_ACCOUNT -u $SF_USER -d $SF_DATABASE -q "
  COPY (SELECT * FROM MARTS.FCT_TRIPS) 
  TO @~/fct_trips_$(date +%Y%m%d).parquet 
  FILE_FORMAT = (TYPE = 'PARQUET')
"

# Upload to S3 (optional)
aws s3 cp ~/fct_trips_*.parquet s3://my-backup-bucket/
```

---

## **Securing Your VPS**

### **Firewall Rules**

```bash
# Allow SSH
ufw allow 22

# Allow Airflow (your IP only)
ufw allow from 203.0.113.0 to any port 8080

# Allow n8n (your IP only)
ufw allow from 203.0.113.0 to any port 5678

# Enable firewall
ufw enable
```

### **Use Reverse Proxy (Nginx)**

Protect Airflow/n8n with basic auth:

```nginx
server {
    listen 80;
    server_name tlc-pipeline.local;

    location / {
        auth_basic "TLC Pipeline";
        auth_basic_user_file /etc/nginx/.htpasswd;
        proxy_pass http://localhost:8080;
    }
}
```

Generate password:
```bash
apt install apache2-utils
htpasswd -c /etc/nginx/.htpasswd admin
```

---

## **Cost Breakdown**

| Provider | Instance | RAM | CPU | Storage | Cost/mo |
|----------|----------|-----|-----|---------|---------|
| DigitalOcean | Basic | 2GB | 2 | 50GB | $6 |
| AWS EC2 | t3.small | 2GB | 2 | 30GB EBS | ~$10 |
| AWS EC2 | t3.medium | 4GB | 2 | 50GB EBS | ~$18 |
| Google Cloud Composer | Standard | - | - | - | $100+ |

**Recommendation for beginners**: DigitalOcean $6/month droplet (or GitHub Actions free tier for dbt-only).

---

## **Troubleshooting**

### **Port already in use**

```bash
# Check what's using port 8080
sudo lsof -i :8080

# Kill the process
kill -9 <PID>
```

### **Disk space running out**

```bash
# Check space
df -h

# Clean Docker
docker system prune -a
```

### **Airflow can't connect to Snowflake**

```bash
# SSH into Airflow container
docker-compose exec airflow-webserver bash

# Test connection
python - <<EOF
import snowflake.connector
conn = snowflake.connector.connect(
    account='XXXX',
    user='admin',
    password='xxx',
    database='TLC_DB'
)
print("✅ Connected!")
conn.close()
EOF
```

---

## **Next Steps**

1. **Deploy** to your chosen VPS
2. **Set up DNS** (optional): Point `tlc-pipeline.yourdomain.com` to your VPS IP
3. **Enable HTTPS**: Use Let's Encrypt + Certbot
4. **Set up monitoring**: Datadog, New Relic, or CloudWatch
5. **Configure backups**: Daily Snowflake exports to S3

---

**Questions?** Check Airflow/Docker docs or open a GitHub issue!
