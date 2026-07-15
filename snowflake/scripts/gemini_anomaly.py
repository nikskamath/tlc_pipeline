"""
gemini_anomaly.py

Airflow-friendly script to call a Gemini-like Responses API to generate anomaly explanations
for trip rows and persist them into MARTS.AI_ANOMALY_FLAGS.

Notes:
- Stores/reads GEMINI_API_KEY and optional GEMINI_API_URL from environment (project .env or Airflow).
- This script is written defensively: if GEMINI_API_KEY is missing, it exits without network calls.
- It does simple batching and inserts AI results as new rows into MARTS.AI_ANOMALY_FLAGS.

WARNING: Do NOT commit your .env with the API key to public repos.
"""

import os
import json
import logging
from typing import List, Dict
import requests
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GEMINI_API_URL = os.environ.get('GEMINI_API_URL', 'https://api.generativelanguage.googleapis.com/v1beta2/models/gemini-1.5-mini:predict')

BATCH_SIZE = int(os.environ.get('GEMINI_BATCH_SIZE', '50'))
MAX_ROWS = int(os.environ.get('GEMINI_MAX_ROWS', '1000'))  # safety cap


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


def fetch_candidate_rows(conn, limit: int = 1000) -> List[Dict]:
    """Fetch a sample of rows to score. This selects recent rows from MARTS.fct_trips.
    Adjust the SQL to target unprocessed rows if needed."""
    sql = f"""
    select
      row_number() over (order by _dbt_loaded_at desc) as __ai_row_id,
      pickup_ts,
      dropoff_ts,
      trip_duration_mins,
      trip_distance,
      passenger_count,
      pickup_borough,
      fare_amount
    from MARTS.fct_trips
    limit {limit}
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return rows
    finally:
        cur.close()


def build_prompts(rows: List[Dict]) -> List[str]:
    prompts = []
    for r in rows:
        # Build a short, structured prompt for each trip
        prompt = (
            f"Trip features:\n"
            f"fare_amount: {r.get('FARE_AMOUNT')}, trip_distance: {r.get('TRIP_DISTANCE')},"
            f" trip_duration_mins: {r.get('TRIP_DURATION_MINS')}, passenger_count: {r.get('PASSENGER_COUNT')},"
            f" pickup_borough: {r.get('PICKUP_BOROUGH')}.\n"
            f"Question: Is this trip anomalous for the pickup borough? Return a JSON object with keys:"
            f" score (0-1), is_anomaly (true/false), explanation (one short sentence)."
        )
        prompts.append(prompt)
    return prompts


def call_gemini_batch(prompts: List[str]) -> List[str]:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set in environment")

    headers = {
        'Authorization': f'Bearer {GEMINI_API_KEY}',
        'Content-Type': 'application/json'
    }

    # We will concatenate prompts into a single request body to avoid many small requests.
    # The exact API shape depends on your Gemini endpoint. This implementation attempts a
    # reasonably generic request payload; adjust GEMINI_API_URL or payload if your endpoint differs.
    combined = "\n\n---\n\n".join(prompts)
    payload = {
        'prompt': combined,
        'max_output_tokens': 1024,
        'temperature': 0.0,
    }

    log.info("Sending batch to Gemini-like API (one combined prompt)...")
    resp = requests.post(GEMINI_API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()

    # Try to parse JSON output; if not structured, fall back to text splitting
    try:
        j = resp.json()
        # Search common shapes
        text = None
        if isinstance(j, dict):
            for k in ('output', 'outputs', 'predictions', 'candidates', 'response'):
                if k in j:
                    val = j[k]
                    if isinstance(val, list):
                        text = '\n---\n'.join([str(x) for x in val])
                    else:
                        text = str(val)
                    break
        if text is None:
            text = json.dumps(j)
    except Exception:
        text = resp.text

    # Split responses back into individual outputs using the separator
    parts = text.split('\n---\n') if '\n---\n' in text else text.split('\n\n---\n\n')
    # Trim to number of prompts
    parts = [p.strip() for p in parts]
    if len(parts) < len(prompts):
        # If API returned a single combined answer, try to split by lines that look like JSON
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        parts = lines[:len(prompts)]

    # Ensure we return same length; pad with empty strings if necessary
    while len(parts) < len(prompts):
        parts.append('')
    return parts[:len(prompts)]


def parse_gemini_output(piece: str) -> Dict:
    """Attempt to parse a JSON object from the model output. Fallback to heuristics.
    Returns dict with keys: score (float), is_anomaly (bool), explanation (str)
    """
    piece = piece.strip()
    # Try direct JSON
    for attempt in (piece, piece.strip('"')):
        try:
            j = json.loads(attempt)
            score = float(j.get('score') or j.get('anomaly_score') or 0)
            is_anomaly = bool(j.get('is_anomaly') or j.get('anomaly'))
            explanation = j.get('explanation') or j.get('reason') or ''
            return {'score': score, 'is_anomaly': is_anomaly, 'explanation': explanation}
        except Exception:
            continue

    # Heuristic parsing: look for 'score: X' patterns
    import re
    m = re.search(r"score\s*[:=]\s*([0-9]*\.?[0-9]+)", piece, re.IGNORECASE)
    score = float(m.group(1)) if m else 0.0
    is_anomaly = bool(re.search(r"\b(true|yes|anomal)\b", piece, re.IGNORECASE))
    # Use first sentence as explanation
    explanation = piece.split('\n')[0][:250]
    return {'score': score, 'is_anomaly': is_anomaly, 'explanation': explanation}


def persist_results(conn, rows: List[Dict], results: List[Dict]):
    """Insert AI results into MARTS.AI_ANOMALY_FLAGS as new rows (append-only).
    """
    cur = conn.cursor()
    try:
        cur.execute("create schema if not exists MARTS")
        cur.execute("""
            create table if not exists MARTS.AI_ANOMALY_FLAGS (
                __ai_row_id number,
                pickup_ts timestamp_ntz,
                pickup_borough varchar,
                anomaly_score float,
                is_anomaly boolean,
                ai_explanation varchar,
                _dbt_loaded_at timestamp_ntz default current_timestamp()
            )
        """)
        # Build VALUES clause
        vals = []
        for r, res in zip(rows, results):
            rid = r.get('__AI_ROW_ID') or r.get('__ai_row_id')
            pickup_ts = r.get('PICKUP_TS')
            borough = r.get('PICKUP_BOROUGH')
            score = res.get('score')
            is_anom = res.get('is_anomaly')
            expl = res.get('explanation')
            # Use parameter markers
            vals.append((rid, pickup_ts, borough, score, is_anom, expl))

        # Insert in a single multi-row statement using executemany
        insert_sql = "insert into MARTS.AI_ANOMALY_FLAGS (__ai_row_id, pickup_ts, pickup_borough, anomaly_score, is_anomaly, ai_explanation) values (%s, %s, %s, %s, %s, %s)"
        cur.executemany(insert_sql, vals)
        conn.commit()
        log.info(f"Inserted {len(vals)} AI anomaly rows into MARTS.AI_ANOMALY_FLAGS")
    finally:
        cur.close()


def main():
    if not GEMINI_API_KEY:
        log.warning("GEMINI_API_KEY not set; aborting AI call. Set it in .env or Airflow connections.")
        return

    conn = get_snowflake_conn()
    try:
        rows = fetch_candidate_rows(conn, limit=min(MAX_ROWS, 1000))
        if not rows:
            log.info("No candidate rows found; exiting")
            return

        # Batch
        all_results = []
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            prompts = build_prompts(batch)
            try:
                pieces = call_gemini_batch(prompts)
            except Exception as e:
                log.exception("Failed to call Gemini API: %s", e)
                # Fallback: mark as not_anomaly with explanation
                pieces = ["{\"score\":0, \"is_anomaly\":false, \"explanation\":\"API call failed\"}" for _ in batch]

            parsed = [parse_gemini_output(p) for p in pieces]
            all_results.extend(parsed)

        persist_results(conn, rows, all_results)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
