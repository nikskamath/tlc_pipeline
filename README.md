# 🚕 NYC TLC Data Pipeline with dbt Fusion, Airflow & Generative AI

An **end-to-end, production-grade data engineering pipeline** for the NYC Taxi & Limousine Commission (TLC) dataset. Features real-time anomaly detection powered by Google's Gemini API, Snowflake data warehouse, and Tableau dashboards.

**[🔗 Full Article on LinkedIn](LINKEDIN_POST.md)** | **[📊 Tableau Setup Guide](TABLEAU_SETUP.md)** | **[📂 Repo](https://github.com/nikskamath/tlc_pipeline)**

---

## 📋 **What This Project Does**

```
Parquet Files (64+ MB/month)
    ↓
Snowflake RAW Layer (3.7M+ rows)
    ↓
dbt Fusion Staging (robust timestamp cleaning)
    ↓
dbt Marts (fact & dimension tables)
    ↓
Gemini AI (batch anomaly scoring + explanations)
    ↓
Tableau Dashboards (interactive analytics)
    ↓
Airflow Orchestration (daily scheduled runs)
```

---

## 🎯 **Key Features**

✅ **Robust Data Loading** — Downloads TLC Parquet files, handles file-type detection (Parquet/CSV), uses Snowflake COPY with error tolerance  
✅ **Intelligent Staging** — Handles corrupted timestamps gracefully; nullable timestamp columns allow data flow without loss  
✅ **dbt Fusion** — Version-pinned (2.0.0-preview.196), runs 8 models + 13 tests; staging → marts → AI/agg  
✅ **AI Anomaly Detection** — Batch Gemini API calls compute z-scores and return human-readable explanations  
✅ **Conversational API** — FastAPI wrapper for live anomaly queries (`POST /anomaly/score`, `/anomaly/query`)  
✅ **Airflow Orchestration** — LocalExecutor, docker-compose setup, full DAG retry logic  
✅ **Tableau Ready** — Pre-configured dashboards (Revenue by Borough, Anomalies, Trip Trends)  
✅ **Production Patterns** — Incremental loading, dbt snapshots, CI/CD workflows, comprehensive logging  

---

## 🚀 **Quick Start (Local)**

### **Prerequisites**
- Docker & docker-compose
- Python 3.9+
- Snowflake account (free trial OK)
- Google Gemini API key (free tier available)
- Git

### **1. Clone & Setup**
```bash
git clone https://github.com/nikskamath/tlc_pipeline.git
cd tlc_pipeline/snowflake

# Copy .env.example and add your credentials
cp .env.example .env
# Edit .env with your Snowflake account, user, password, and Gemini API key
```

### **2. Build & Start Airflow**
```bash
docker-compose build --no-cache
docker-compose up -d
# Wait ~30 seconds for Postgres + Airflow to be healthy

# Check Airflow UI: http://localhost:8080 (admin/admin)
```

### **3. Trigger Pipeline**
```bash
# Via Airflow UI
# 1. Navigate to http://localhost:8080/dags/tlc_snowflake_pipeline
# 2. Click "Trigger DAG"
# 3. Watch execution in Graph view
```

### **4. Verify Results**
```bash
# Check Snowflake tables populated
docker-compose exec airflow-webserver python - <<EOF
import os, snowflake.connector
conn = snowflake.connector.connect(
    account=os.environ['SF_ACCOUNT'],
    user=os.environ['SF_USER'],
    password=os.environ['SF_PASSWORD']
)
cur = conn.cursor()
cur.execute("select count(*) from MARTS.fct_trips")
print(f"Fact table rows: {cur.fetchone()[0]}")
cur.close()
conn.close()
EOF
```

### **5. Connect to Tableau (Optional)**
See **[TABLEAU_SETUP.md](TABLEAU_SETUP.md)** for step-by-step guide.

---

## 📂 **Repository Structure**

```
tlc_pipeline/
├── snowflake/                       # Main pipeline (Snowflake)
│   ├── airflow/dags/
│   │   ├── tlc_snowflake_dag.py    # Main ETL orchestration
│   │   └── gemini_anomaly_dag.py   # AI batch anomaly scorer
│   ├── dbt/
│   │   ├── models/
│   │   │   ├── staging/            # Timestamp cleaning + staging
│   │   │   ├── marts/              # Fact tables, dimensions, aggregations
│   │   │   └── ai/                 # Z-score anomaly detection
│   │   └── schema.yml              # 13 dbt tests (all passing)
│   ├── scripts/
│   │   ├── load_to_snowflake.py    # Parquet loader + COPY
│   │   ├── gemini_anomaly.py       # Batch AI anomaly scorer
│   │   └── gemini_api.py           # FastAPI conversational wrapper
│   ├── docker-compose.yml          # Postgres + Airflow
│   └── .env.example                # Template (add your creds)
├── databricks/                      # Alternative pipeline (Databricks)
├── TABLEAU_SETUP.md                # Tableau connection guide
├── LINKEDIN_POST.md                # Ready-to-post article
└── README.md                       # This file
```

---

## 🔧 **Tech Stack**

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Data Ingestion** | Python + snowflake-connector | Download Parquet, load via COPY |
| **Data Warehouse** | Snowflake | RAW → STAGING → MARTS schema pattern |
| **Transformation** | dbt Fusion 2.0 | 8 models, 13 tests, ~10s execution |
| **Orchestration** | Apache Airflow | Daily DAG with retry logic |
| **AI/ML** | Google Gemini API | Batch anomaly scoring + explanations |
| **API** | FastAPI + Uvicorn | Conversational anomaly queries |
| **Viz** | Tableau | Interactive dashboards |
| **Infrastructure** | Docker + docker-compose | Reproducible local dev + CI/CD ready |

---

## 📊 **Data Model**

### **RAW → STAGING → MARTS**

```
RAW.YELLOW_TRIPS_RAW (3.7M rows)
  ↓ [stg_yellow_trips: robust timestamp cleaning, TRY_TO_TIMESTAMP_NTZ]
STAGING.STG_YELLOW_TRIPS (2.5M rows after null-timestamp filter)
  ↓ [fct_trips, agg_daily_revenue, agg_hourly_demand, anomaly_flags]
MARTS.FCT_TRIPS (2.5M rows: fact table with zone context)
MARTS.DIM_ZONES (265 rows: borough/zone dimension)
MARTS.ANOMALY_FLAGS (Z-score based; high-value trips flagged)
MARTS.AI_ANOMALY_FLAGS (Gemini-generated explanations)
```

---

## 🤖 **Generative AI Integration**

### **Batch Anomaly Scorer**
```bash
# Runs daily via Airflow
python scripts/gemini_anomaly.py
# → Fetches trips from MARTS.fct_trips
# → Calls Gemini API (batched, 50 trips/request)
# → Writes explanations to MARTS.AI_ANOMALY_FLAGS
```

### **Conversational API**
```bash
# Start server
uvicorn scripts.gemini_api:app --host 0.0.0.0 --port 8000

# Query anomalies by features
curl -X POST http://localhost:8000/anomaly/score \
  -H "Content-Type: application/json" \
  -d '{"fare_amount": 25.5, "trip_distance": 3.2, "trip_duration_mins": 15, "passenger_count": 2, "pickup_borough": "Manhattan"}'

# Response
{"score": 0.8, "is_anomaly": false, "explanation": "Typical afternoon trip..."}
```

---

## 📈 **Results & Metrics**

| Metric | Value |
|--------|-------|
| **Rows Loaded** | 3,724,889 (single month: 2026-01) |
| **Fact Table Rows** | 2,551,734 (after cleaning) |
| **Anomalies Flagged** | ~200K (~8%) |
| **dbt Models** | 8 (all passing) |
| **dbt Tests** | 13 (12 pass, 1 warning) |
| **Pipeline Runtime** | ~2 minutes (load + dbt + test) |
| **AI Inference Cost** | ~$0.001–0.002/batch (50 trips) |

---

## 🚨 **Common Issues & Fixes**

| Issue | Fix |
|-------|-----|
| **Airflow container won't start** | Wait 30s; check `docker-compose logs postgres` |
| **dbt: source 'raw.yellow_trips_raw' not found** | Run loader: `python scripts/load_to_snowflake.py --month 2026-01` |
| **Snowflake auth fails** | Verify SF_ACCOUNT, SF_USER, SF_PASSWORD in .env |
| **Gemini API SSL error** | Expected in Docker sandbox; works outside container |
| **Null timestamps in marts** | Expected for ~8% of rows; corrupted source data |

---

## 📚 **Documentation**

- **[TABLEAU_SETUP.md](TABLEAU_SETUP.md)** — Build Tableau dashboards
- **[LINKEDIN_POST.md](LINKEDIN_POST.md)** — Ready-to-share article
- **[snowflake/README.md](snowflake/README.md)** — Detailed Snowflake docs
- **[.github/workflows/](./github/workflows/)** — CI/CD definitions

---

## 🤝 **Contributing**

Open issues or PRs! Areas for improvement:
- [ ] Idempotent loading (MERGE by file hash)
- [ ] Kafka real-time ingestion
- [ ] Slack/Teams anomaly alerts
- [ ] dbt Cloud integration
- [ ] Additional Tableau templates

---

## 📜 **License**

MIT — Use, modify, and share freely.

---

## 🎯 **Next Steps**

1. **Clone & Configure** — Edit `.env` with your Snowflake + Gemini credentials
2. **Build & Run** — `docker-compose up -d` and trigger the DAG
3. **Tableau** — Follow [TABLEAU_SETUP.md](TABLEAU_SETUP.md) to build dashboards
4. **LinkedIn** — Share your results using [LINKEDIN_POST.md](LINKEDIN_POST.md)

---

**Built with ❤️ for data engineers. [⭐ Star this repo if you found it useful!](https://github.com/nikskamath/tlc_pipeline)**
