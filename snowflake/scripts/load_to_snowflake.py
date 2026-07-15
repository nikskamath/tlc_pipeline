"""
load_to_snowflake.py
────────────────────
Downloads NYC TLC yellow taxi Parquet files and loads into Snowflake.
Includes all fixes from troubleshooting: robust .env loading, browser
User-Agent for CDN, S3 fallback, optional column handling.
"""

import os
import sys
import argparse
import logging
import requests
import snowflake.connector
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _load_env():
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    script_dir = Path(__file__).resolve().parent
    for d in [script_dir, script_dir.parent, script_dir.parent.parent]:
        env_file = d / ".env"
        if env_file.exists():
            load_dotenv(dotenv_path=env_file, override=False)
            log.info(f"Loaded .env from: {env_file}")
            return
    log.warning(".env not found in script dir, parent, or grandparent.")

_load_env()

CDN_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
S3_BASE  = "https://nyc-tlc.s3.amazonaws.com/trip+data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_S3  = "https://nyc-tlc.s3.amazonaws.com/misc/taxi+_zone_lookup.csv"
TMP_DIR  = Path("/tmp/tlc")
TMP_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/octet-stream,*/*",
}

LATEST_AVAILABLE = "2025-03"


def get_target_month(month_str=None):
    if month_str:
        return month_str
    log.warning(f"No --month given. Defaulting to {LATEST_AVAILABLE}.")
    return LATEST_AVAILABLE


def download_file(url, dest, fallback_url=None):
    for attempt in ([url] + ([fallback_url] if fallback_url else [])):
        log.info(f"Downloading {attempt}")
        try:
            with requests.get(attempt, stream=True, timeout=180, headers=HEADERS) as r:
                if r.status_code in (403, 404):
                    log.warning(f"{r.status_code} on {attempt} — trying fallback...")
                    continue
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(65536):
                        f.write(chunk)
                log.info(f"Saved {Path(dest).stat().st_size/1_000_000:.1f} MB -> {dest}")
                return dest
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP error: {e}")
            continue
    raise RuntimeError(f"Could not download from {url} or {fallback_url}")


def get_snowflake_conn():
    missing = [v for v in ["SF_ACCOUNT", "SF_USER", "SF_PASSWORD"] if not os.environ.get(v)]
    if missing:
        log.error(f"Missing env vars: {missing}")
        sys.exit(1)
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        database=os.environ.get("SF_DATABASE", "TLC_DB"),
        warehouse=os.environ.get("SF_WAREHOUSE", "TLC_WH"),
        schema="RAW",
        role=os.environ.get("SF_ROLE", "SYSADMIN"),
    )


def load_trips(month, conn):
    cdn_url = f"{CDN_BASE}/yellow_tripdata_{month}.parquet"
    s3_url  = f"{S3_BASE}/yellow_tripdata_{month}.parquet"
    fpath   = TMP_DIR / f"yellow_tripdata_{month}.parquet"
    download_file(cdn_url, fpath, fallback_url=s3_url)

    cur = conn.cursor()
    try:
        cur.execute(f"PUT file://{fpath} @TLC_DB.RAW.TLC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        # Choose COPY format based on file extension
        fname = Path(fpath).name
        if fname.lower().endswith('.parquet'):
            file_format = "(TYPE='PARQUET' SNAPPY_COMPRESSION=TRUE)"
            src_path = f"@TLC_DB.RAW.TLC_STAGE/{fname}"
        else:
            # Assume CSV (possibly .gz) fallback
            file_format = "(TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1)"
            src_path = f"@TLC_DB.RAW.TLC_STAGE/{fname}"

        cur.execute(f"""
                COPY INTO TLC_DB.RAW.YELLOW_TRIPS_RAW
                FROM {src_path}
                FILE_FORMAT = {file_format}
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
                PURGE = FALSE;
            """)
        log.info(f"COPY INTO result: {cur.fetchall()}")
    finally:
        cur.close()


def load_zones(conn):
    fpath = TMP_DIR / "taxi_zone_lookup.csv"
    download_file(ZONE_URL, fpath, fallback_url=ZONE_S3)
    cur = conn.cursor()
    try:
        cur.execute(f"PUT file://{fpath} @TLC_DB.RAW.TLC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        cur.execute("""
            COPY INTO TLC_DB.RAW.TAXI_ZONE_LOOKUP
            FROM @TLC_DB.RAW.TLC_STAGE/taxi_zone_lookup.csv
            FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE'
            PURGE = FALSE;
        """)
        log.info("Zone lookup loaded.")
    finally:
        cur.close()


def main(month=None, load_zones_flag=False):
    target = get_target_month(month)
    log.info(f"Target month: {target}")
    conn = get_snowflake_conn()
    try:
        load_trips(target, conn)
        if load_zones_flag:
            load_zones(conn)
    finally:
        conn.close()
    log.info("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", type=str, default=None)
    parser.add_argument("--load-zones", action="store_true", default=False)
    args = parser.parse_args()
    main(args.month, args.load_zones)
