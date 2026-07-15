# n8n Email Alerts Setup for TLC Pipeline

Complete guide to set up email notifications for Airflow DAG failures/successes using n8n.

---

## **Quick Start: 5 Minutes**

### **1. Update `.env`**

Add two new variables to your `.env` file:

```bash
# n8n webhook (internal Docker URL)
N8N_WEBHOOK_URL=http://n8n:5678/webhook/tlc-pipeline-alert

# Your email address for alerts
ALERT_EMAIL=your-email@gmail.com
```

### **2. Start n8n + Airflow**

```bash
cd snowflake
docker-compose up -d
```

Wait ~30 seconds for n8n to be healthy:
```bash
docker-compose ps
```

Expected:
- `airflow-webserver`: running
- `airflow-scheduler`: running
- `n8n`: running

### **3. Configure Gmail in n8n**

1. Open n8n: http://localhost:5678
2. Click **+ Create new workflow**
3. Click **Add** on the left panel
4. Search and add **Gmail** node
5. Click **Connect** → **Create new credential**
6. Sign in to your Gmail account
7. Grant n8n permission
8. Click **Save**

**⚠️ Gmail App Password Setup (required):**
- Go to https://myaccount.google.com/apppasswords
- Select **Mail** and **Mac** (or your device)
- Copy the 16-char password
- Paste it into n8n Gmail credential when prompted

### **4. Import the n8n Workflow**

1. In n8n, click **File → Import from URL**
2. Paste this URL (or use the local file):
   ```
   file:///opt/airflow/n8n/n8n_gmail_workflow.json
   ```
3. Click **Import**
4. The workflow will appear with Webhook + Gmail nodes

### **5. Configure n8n Workflow**

1. Click on the **Webhook** node
2. Copy the **URL** shown in the node (e.g., `http://localhost:5678/webhook/tlc-pipeline-alert`)
3. Update `.env`: Set `N8N_WEBHOOK_URL=<your-copied-url>`
4. Click on **Gmail nodes** (both failure & success)
5. Verify **sendTo** field uses `{{ $env.ALERT_EMAIL }}`
6. Click **Save workflow**

### **6. Update Airflow DAGs**

Edit your DAGs to use the n8n callback. Example:

```python
# At the top of your DAG file
from scripts.n8n_alerts import on_dag_success, on_dag_failure

# In the DAG definition
default_args = {
    'owner': 'airflow',
    'on_failure_callback': on_dag_failure,
    'on_success_callback': on_dag_success,
    # ... other args
}

dag = DAG(
    'tlc_snowflake_pipeline',
    default_args=default_args,
    # ... rest of DAG
)
```

**Already done for:**
- `tlc_snowflake_dag.py` ✅
- `gemini_anomaly_dag.py` ✅

### **7. Test It!**

Trigger a DAG:
1. Open Airflow: http://localhost:8080
2. Click on `tlc_snowflake_pipeline` DAG
3. Click **Trigger DAG**
4. Check your email inbox for success/failure alerts

---

## **How It Works**

```
Airflow DAG
  ↓
  [DAG completes: success OR failure]
  ↓
  [Airflow calls on_success_callback OR on_failure_callback]
  ↓
  [n8n_alerts.py sends POST to n8n webhook]
  ↓
  [n8n webhook receives payload]
  ↓
  [n8n checks status: 'success' or 'failure']
  ↓
  [n8n routes to Gmail node]
  ↓
  [Email sent to ALERT_EMAIL]
```

---

## **Payload Sent to n8n**

```json
{
  "dag_id": "tlc_snowflake_pipeline",
  "status": "success",
  "run_id": "manual__2026-07-15T16:30:00+00:00",
  "timestamp": "2026-07-15T16:35:22.123Z",
  "month": "2026-01",
  "execution_date": "2026-07-15 16:30:00",
  "task_id": "dbt_run"
}
```

---

## **Troubleshooting**

### **n8n container won't start**

```bash
docker-compose logs n8n
```

If volume permission error:
```bash
docker-compose down
docker volume rm snowflake_n8n-data
docker-compose up -d n8n
```

### **Gmail node shows "Unauthorized"**

1. Check that Gmail App Password is correct (16 chars, no spaces)
2. Verify you have 2FA enabled on Gmail
3. Generate a new App Password: https://myaccount.google.com/apppasswords

### **Webhook URL not working**

1. Check n8n is running: `curl http://localhost:5678/health`
2. Copy exact webhook URL from n8n UI (not hardcoded)
3. Update `.env`: `N8N_WEBHOOK_URL=<your-url>`
4. Restart containers: `docker-compose restart airflow-webserver airflow-scheduler`

### **DAG triggered but no email received**

1. Check Airflow logs: `docker-compose logs airflow-scheduler | grep n8n`
2. Check n8n logs: `docker-compose logs n8n | grep "Incoming webhook"`
3. Verify ALERT_EMAIL is correct
4. Test Gmail manually: Send test email from Gmail settings

### **"Connection refused" when contacting n8n**

- If running Airflow locally (not Docker): Use `http://127.0.0.1:5678` instead of `http://n8n:5678`
- If Airflow in Docker: Use internal DNS `http://n8n:5678`

---

## **Advanced Configuration**

### **Custom Email Templates**

Edit the Gmail nodes in n8n:

- **Failure email**: Red background, urgent tone
- **Success email**: Green background, celebratory tone

Templates use Handlebars syntax: `{{ $json.dag_id }}`, `{{ $json.month }}`, etc.

### **Send to Multiple Recipients**

In Gmail node, set **sendTo** to:
```
{{ $env.ALERT_EMAIL }}, manager@company.com, team@company.com
```

### **Add Slack Alerts Instead**

Import a different workflow or add a Slack node:

1. Add **Slack** node after webhook
2. Create Slack credential (bot token)
3. Set channel: `#alerts`
4. Message: `DAG {{ $json.dag_id }} failed!`

---

## **Production Deployment**

For VPS/cloud deployment:

1. **Use n8n Cloud** (managed): https://www.n8n.cloud
2. **Or deploy n8n separately** from Airflow
3. **Use Ngrok for tunneling**: `ngrok http 5678`
4. **Set environment variables**:
   - `N8N_WEBHOOK_TUNNEL_URL=https://abc123.ngrok.io`
   - `AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=s3_bucket` (for cloud logs)

---

## **Questions?**

- **n8n Docs**: https://docs.n8n.io
- **Airflow Docs**: https://airflow.apache.org/docs
- **Gmail OAuth Issues**: https://support.google.com/accounts/answer/6010255
