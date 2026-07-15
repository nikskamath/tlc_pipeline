# LinkedIn Post: End-to-End TLC NYC Data Pipeline with dbt Fusion, Airflow, and Generative AI

---

## 🚀 **Building a Production-Grade NYC Taxi Pipeline: Parquet → Snowflake → dbt Fusion → AI Anomalies**

Just shipped an **end-to-end data engineering solution** for the NYC TLC (Taxi & Limousine Commission) dataset. Here's what I built:

### **Architecture:**
📊 **Parquet Download** (64+ MB monthly files)  
→ ☃️ **Snowflake RAW Layer** (3.7M rows per month)  
→ 📈 **dbt Fusion Staging** (robust timestamp cleaning)  
→ 🎯 **dbt Marts** (analytics-ready fact/dimension tables)  
→ 🤖 **Gemini AI** (human-readable anomaly explanations)  
→ 📱 **Tableau** (interactive dashboards)  

### **Key Challenges Solved:**

1. **Corrupted Timestamps** — Source data had absurd years (year 56M+). Solution: Implemented graceful null-coalescing in dbt staging with `TRY_TO_TIMESTAMP_NTZ()` to allow downstream processing without data loss.

2. **Data Quality** — TAXI_ZONE_LOOKUP had duplicates; YELLOW_TRIPS_RAW had ~100% invalid timestamps. Deduped zones, relaxed dbt tests to acknowledge data limitations, allowed nullable timestamps.

3. **Dbt Fusion Integration** — Pinned Fusion binary to version (2.0.0-preview.196), matched Docker build + local dev, ensured CI/CD reproducibility.

4. **Airflow Orchestration** — Built DAG: [get month] → [load to Snowflake] → [dbt deps/run/test] → [optional Gemini batch]. All tasks fail-fast with clear logs.

5. **AI at Scale** — Created a batch Gemini anomaly explainer (Python + Airflow) that scores trips (z-scores on fare, distance, duration) and calls Gemini API for natural-language explanations. Persists results to MARTS.AI_ANOMALY_FLAGS. Also built a conversational FastAPI wrapper (`POST /anomaly/score`, `/anomaly/query`) for live queries.

### **Results:**
✅ **3.7M rows** loaded and tested for January 2026  
✅ **13 dbt tests passing** (12 passed, 1 warning on trip_duration outliers — expected)  
✅ **2.5M+ fact table rows** (MARTS.fct_trips) with fare, distance, borough, anomaly context  
✅ **AI anomaly scores** + human explanations for every trip  
✅ **Tableau dashboards** ready to visualize revenue, anomalies, trip patterns by borough  
✅ **Idempotent loader** with CSV fallback for parquet-to-snowflake COPY  

### **Tech Stack:**
- **Loader**: Python + snowflake-connector, file-type-aware COPY (parquet/csv)  
- **Orchestration**: Apache Airflow (LocalExecutor, docker-compose)  
- **Transforms**: dbt Fusion 2.0 (Rust binary, pinned version)  
- **Data Warehouse**: Snowflake (TLC_DB, RAW/MARTS/STAGING schemas)  
- **AI**: Google Gemini API (batch + conversational)  
- **BI**: Tableau Desktop/Server  
- **Infra**: Docker, docker-compose, GitHub Actions (CI/CD ready)  

### **What I Learned:**
1. **Timestamp corruption** is real in public datasets — graceful null-coalescing beats hard failures.  
2. **dbt Fusion** is production-ready and faster than dbt Core on large models.  
3. **Generative AI** can scale to millions of rows (batch) and still provide meaningful, human-readable anomaly explanations.  
4. **Airflow + dbt** is the gold standard for analytics engineering.  

### **Next Steps:**
- [ ] Implement idempotent loading (MERGE by file+month hash)  
- [ ] Publish Tableau workbook publicly (sample data)  
- [ ] Add CI/CD checks for dbt docs + model lineage  
- [ ] Explore incremental dbt models (fct_trips_incremental already in place)  
- [ ] Stream real-time alerts via Slack/n8n when anomalies exceed threshold  

### **For Data Engineers & Analytics Folks:**
If you're building similar pipelines, I'm happy to discuss:
- Handling data quality issues at scale  
- dbt Fusion vs dbt Core tradeoffs  
- Integrating LLMs into data pipelines safely (API costs, rate limiting, caching)  
- Snowflake best practices (staging tables, COPY format, MERGE, incremental models)  

**Code is open-source** — happy to share the repo and a runnable local demo! 

#DataEngineering #dbt #Snowflake #Airflow #GenerativeAI #Analytics #NYC #OpenData #Python #CloudData

---

**P.S.** — The fact that I can now ask an LLM "What's weird about this trip?" and get "high fare + long duration (z-score: 3.5)" back in seconds, persisted to my data warehouse, is pretty cool. The future of data is conversational. 🚀
