"""
load_to_databricks.py
────────────────────
Downloads NYC TLC yellow taxi Parquet files and uploads to DBFS.
"""

import os, sys, argparse, logging, requests, base64
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
    log.warning(".env not found.")

_load_env()

CDN_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
S3_BASE  = "https://nyc-tlc.s3.amazonaws.com/trip+data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_S3  = "https://nyc-tlc.s3.amazonaws.com/misc/taxi+_zone_lookup.csv"
TMP_DIR  = Path("/tmp/tlc")
TMP_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
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
                log.info(f"Saved {Path(dest).stat().st_size/1_000_000:.1f} MB")
                return dest
        except requests.exceptions.HTTPError as e:
            log.warning(f"HTTP error: {e}")
            continue
    raise RuntimeError(f"Could not download from {url} or {fallback_url}")


def upload_to_dbfs(local_path, dbfs_path):
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    headers = {"Authorization": f"Bearer {token}"}

    with open(local_path, "rb") as f:
        data = f.read()

    # Chunked upload for files over 1MB (DBFS API limit per request)
    create_resp = requests.post(
        f"{host}/api/2.0/dbfs/create",
        headers=headers,
        json={"path": dbfs_path, "overwrite": True},
    )
    create_resp.raise_for_status()
    handle = create_resp.json()["handle"]

    chunk_size = 1_000_000
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i + chunk_size]
        b64_chunk = base64.b64encode(chunk).decode()
        requests.post(
            f"{host}/api/2.0/dbfs/add-block",
            headers=headers,
            json={"handle": handle, "data": b64_chunk},
        ).raise_for_status()

    requests.post(
        f"{host}/api/2.0/dbfs/close",
        headers=headers,
        json={"handle": handle},
    ).raise_for_status()
    log.info(f"Uploaded to {dbfs_path}")


def load_trips(month):
    url = f"{CDN_BASE}/yellow_tripdata_{month}.parquet"
    s3_url = f"{S3_BASE}/yellow_tripdata_{month}.parquet"
    fpath = TMP_DIR / f"yellow_tripdata_{month}.parquet"
    download_file(url, fpath, fallback_url=s3_url)
    upload_to_dbfs(fpath, f"/FileStore/tlc/raw/yellow_tripdata_{month}.parquet")


def load_zones():
    fpath = TMP_DIR / "taxi_zone_lookup.csv"
    download_file(ZONE_URL, fpath, fallback_url=ZONE_S3)
    upload_to_dbfs(fpath, "/FileStore/tlc/taxi_zone_lookup.csv")


def main(month=None, load_zones_flag=False):
    target = get_target_month(month)
    log.info(f"Target month: {target}")
    load_trips(target)
    if load_zones_flag:
        load_zones()
    log.info("Done. Now run the Bronze/Silver/Gold notebooks in Databricks.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", type=str, default=None)
    parser.add_argument("--load-zones", action="store_true", default=False)
    args = parser.parse_args()
    main(args.month, args.load_zones)
