"""
Airflow callback utility to send DAG execution alerts to n8n.
Triggered on DAG success or failure.
"""

import os
import json
import requests
from datetime import datetime
from airflow.models import Variable


def get_n8n_webhook_url():
    """Get n8n webhook URL from environment or Airflow variables."""
    url = os.getenv('N8N_WEBHOOK_URL', 'http://n8n:5678/webhook/tlc-pipeline-alert')
    return url


def send_alert_to_n8n(context, status='success'):
    """
    Send DAG execution alert to n8n webhook.
    
    Args:
        context: Airflow task/DAG context
        status: 'success' or 'failure'
    """
    try:
        dag_run = context.get('dag_run')
        task_instance = context.get('task_instance')
        
        # Extract key information
        dag_id = context['dag'].dag_id if 'dag' in context else 'unknown'
        run_id = dag_run.run_id if dag_run else 'unknown'
        timestamp = datetime.utcnow().isoformat() + 'Z'
        
        # Extract month from DAG run conf if available
        month = 'unknown'
        if dag_run and dag_run.conf:
            month = dag_run.conf.get('month', 'unknown')
        
        # Build alert payload
        payload = {
            'dag_id': dag_id,
            'status': status,
            'run_id': run_id,
            'timestamp': timestamp,
            'month': month,
            'execution_date': str(dag_run.execution_date) if dag_run else 'unknown',
            'task_id': task_instance.task_id if task_instance else 'unknown'
        }
        
        # Add failure details if applicable
        if status == 'failure' and task_instance:
            payload['exception'] = str(task_instance.task_type)
            payload['log_url'] = task_instance.log_url if hasattr(task_instance, 'log_url') else 'N/A'
        
        # Send to n8n
        webhook_url = get_n8n_webhook_url()
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10
        )
        
        if response.status_code in [200, 201]:
            print(f"✅ Alert sent to n8n: {status} for {dag_id}")
        else:
            print(f"⚠️ n8n webhook returned {response.status_code}: {response.text}")
    
    except Exception as e:
        print(f"❌ Error sending alert to n8n: {str(e)}")
        # Don't raise; we don't want n8n failure to fail the DAG


def on_dag_success(context):
    """DAG success callback."""
    send_alert_to_n8n(context, status='success')


def on_dag_failure(context):
    """DAG failure callback."""
    send_alert_to_n8n(context, status='failure')
