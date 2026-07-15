"""
Simple FastAPI conversational wrapper for the Gemini anomaly explainer.

Endpoints:
- POST /anomaly/query  {"row_id": <int>}  -> queries MARTS.fct_trips for row_id and returns AI result
- POST /anomaly/score  {"trip": {...}}   -> accepts trip features directly and returns AI result

Notes:
- No auth (internal-only) as requested. Bind to 0.0.0.0:8000 when running with uvicorn.
- Reads GEMINI_API_KEY and Snowflake env vars from environment (.env or Airflow env).
- Reuses the same Gemini prompt & parsing logic as scripts/gemini_anomaly.py
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import os
import snowflake.connector
import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Gemini Anomaly API")

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GEMINI_API_URL = os.environ.get('GEMINI_API_URL', 'https://api.generativelanguage.googleapis.com/v1beta2/models/gemini-1.5-mini:predict')

class TripFeatures(BaseModel):
    fare_amount: Optional[float]
    trip_distance: Optional[float]
    trip_duration_mins: Optional[float]
    passenger_count: Optional[int]
    pickup_borough: Optional[str]

class QueryRow(BaseModel):
    row_id: int


def call_gemini(prompt: str) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError('GEMINI_API_KEY missing')
    headers = {'Authorization': f'Bearer {GEMINI_API_KEY}', 'Content-Type': 'application/json'}
    payload = {'prompt': prompt, 'max_output_tokens': 512, 'temperature': 0.0}
    resp = requests.post(GEMINI_API_URL, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    try:
        return resp.json()
    except Exception:
        return resp.text


def build_prompt_from_features(f: Dict[str, Any]) -> str:
    return (
        f"Trip features:\nfare_amount: {f.get('fare_amount')}, trip_distance: {f.get('trip_distance')}, "
        f"trip_duration_mins: {f.get('trip_duration_mins')}, passenger_count: {f.get('passenger_count')}, "
        f"pickup_borough: {f.get('pickup_borough')}\n"
        "Question: Is this trip anomalous for the pickup borough? Return JSON: {score:0-1, is_anomaly: true/false, explanation: '...'}"
    )


def get_snowflake_conn():
    missing = [v for v in ("SF_ACCOUNT", "SF_USER", "SF_PASSWORD") if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing Snowflake env vars: {missing}")
    return snowflake.connector.connect(
        account=os.environ['SF_ACCOUNT'],
        user=os.environ['SF_USER'],
        password=os.environ['SF_PASSWORD'],
        warehouse=os.environ.get('SF_WAREHOUSE'),
        database=os.environ.get('SF_DATABASE'),
        role=os.environ.get('SF_ROLE'),
    )


def parse_response(resp) -> Dict[str, Any]:
    # Simplified parsing: try JSON, else return text
    if isinstance(resp, dict):
        return resp
    try:
        return json.loads(resp)
    except Exception:
        return {'raw': str(resp)}

@app.post('/anomaly/score')
def anomaly_score(trip: TripFeatures):
    prompt = build_prompt_from_features(trip.dict())
    try:
        resp = call_gemini(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return parse_response(resp)

@app.post('/anomaly/query')
def anomaly_query(q: QueryRow):
    conn = get_snowflake_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"select fare_amount, trip_distance, trip_duration_mins, passenger_count, pickup_borough from MARTS.fct_trips where ROW_NUMBER() over (order by _dbt_loaded_at desc) = {q.row_id}")
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail='row not found')
        cols = [c[0] for c in cur.description]
        feat = dict(zip(cols, row))
    finally:
        cur.close()
        conn.close()
    prompt = build_prompt_from_features(feat)
    try:
        resp = call_gemini(prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return parse_response(resp)

# Run with: uvicorn scripts.gemini_api:app --host 0.0.0.0 --port 8000
