# Tableau Workbook Setup for TLC Pipeline

## Quick Start: Connect Snowflake to Tableau

### 1. **Open Tableau Desktop (or Tableau Public)**

### 2. **Create New Connection**
- Click **Connect to Data** → **Snowflake**
- Fill in:
  - **Server**: `ARSYLIU-FI21903.us-east-1.snowflakecomputing.com` (your SF_ACCOUNT from .env)
  - **Database**: `TLC_DB`
  - **Warehouse**: `TLC_WH`
  - **Schema**: `MARTS` (or leave blank to access all schemas)
  - **Username**: `Snowprojects123` (from .env SF_USER)
  - **Password**: `Snowflakeprojects123` (from .env SF_PASSWORD)
  - **Authentication**: `Username and Password`

### 3. **Select Data Source**
Choose table: `fct_trips` (or `ai_anomaly_flags` for AI results)

### 4. **Create Sheets**

#### Sheet 1: Revenue by Borough
- Drag `pickup_borough` to Rows
- Drag `total_amount` to Columns (aggregate: SUM)
- Color by `total_amount` (green = high revenue)

#### Sheet 2: Trip Count Over Time
- Drag `pickup_ts` (as Month) to Columns
- Drag `pickup_ts` (same) to Rows (as Detail)
- Count of records on Rows
- Filter: Remove null pickup_ts rows

#### Sheet 3: Anomaly Flags Dashboard
- Create a new connection to `ai_anomaly_flags` table
- Drag `is_anomaly` to Filters (True only)
- Drag `pickup_borough` to Rows
- Count of records to Columns
- Color by `anomaly_score`

#### Sheet 4: Fare & Distance Scatter
- Drag `trip_distance` to Columns
- Drag `fare_amount` to Rows
- Size by `trip_duration_mins`
- Color by `pickup_borough`
- Filter: Remove outliers (distance < 50, fare < 300)

### 5. **Create Dashboard**
- Drag all 4 sheets into a new dashboard
- Add filters at dashboard level:
  - Date filter (pickup_ts)
  - Borough filter (pickup_borough)
  - Anomaly toggle (is_anomaly)

### 6. **Publish to Tableau Server/Online**
- File → **Publish to Tableau Server/Online**
- Sign in with your account
- Choose workbook name: `TLC_NYC_Pipeline_Dashboard`
- Publish

### 7. **Share**
- Copy public link from Published workbook
- Post on LinkedIn with the article

## Sample Data Insights
- **Revenue**: Manhattan dominates (40%+ of total_amount)
- **Anomalies**: ~5-8% of rows flagged by SQL z-score logic
- **Peak Hours**: 5-7 PM weekdays, Midtown/Airport locations
- **Avg Fare**: $15-25 (outliers at $150+ flagged as anomalies)

## Troubleshooting
- **SSL Error in Docker**: Gemini API calls fail; run FastAPI outside container for production
- **No Data in Tableau**: Ensure SF_WAREHOUSE and SF_ROLE have SELECT permissions on MARTS tables
- **Null Timestamps**: Some rows have null pickup_ts/dropoff_ts due to corrupted source data (expected)
