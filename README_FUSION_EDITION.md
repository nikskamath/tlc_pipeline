# NYC TLC Taxi Pipeline — dbt Fusion Edition

## Why this version exists

The previous version pinned everything to `dbt-core`, uninstalling
Fusion entirely. This version does the opposite: **dbt Fusion runs
everywhere** — your local VS Code extension, the Airflow Docker
container, and CI/CD — all pinned to the exact same Fusion build
number.

```
dbt Fusion == 2.0.0-preview.190   (see FUSION_VERSION.txt)
```

## Why this works now (and didn't before)

dbt Fusion is a standalone Rust binary, not a Python package. It
installs via a curl script, not pip, and it ships its own explicit
version-pin command:

```bash
dbt system update --version 2.0.0-preview.190
```

Because that pin command exists, the exact same version can be
installed in three places from one source of truth:

1. **Locally** — the dbt VS Code extension installs Fusion for you.
   To align it with the project pin, run the command above in your
   terminal (the extension picks up whatever version is on PATH).
2. **Docker** — `Dockerfile.airflow` runs the identical curl install
   + pin command at build time, and fails the build if the resulting
   version doesn't match `FUSION_VERSION.txt`.
3. **CI/CD** — `.github/workflows/*.yml` run the same two commands
   before any `dbt` command.

`FUSION_VERSION.txt` is the single file you update to upgrade Fusion
project-wide. Change it, rebuild the Docker image, and re-run the
local pin command — all three environments move together.

## What changed from the dbt-core version

| | dbt-core version | Fusion version |
|---|---|---|
| Install method | `pip install dbt-core==1.10.19` | `curl ... \| sh -s -- --update` then `dbt system update --version ...` |
| Where it lives | Python virtualenv | Standalone binary on PATH |
| VS Code extension | dbt Power User (community) | dbt (official, dbtLabsInc) |
| schema.yml test syntax | Inline arguments | Arguments nested under `arguments:` key |
| require-dbt-version | `>=1.10.0,<1.11.0` | `>=2.0.0,<3.0.0` |
| Python dbt models (Snowpark) | Supported | Not used in this project either way — anomaly detection is pure SQL |

## Quick start

```bash
# 1. Install Fusion locally (if not already via the VS Code extension)
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --update
dbt system update --version $(cat snowflake/FUSION_VERSION.txt)
dbt --version   # confirm it matches

# 2. Python deps (Airflow providers, loader script, AI agent — NOT dbt)
cd snowflake
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Verify everything lines up
python scripts/verify_versions.py

# 4. Continue as normal
cp .env.example .env
python scripts/load_to_snowflake.py --month 2024-01 --load-zones
cd dbt && dbt deps --profiles-dir . && dbt run --profiles-dir .
```
