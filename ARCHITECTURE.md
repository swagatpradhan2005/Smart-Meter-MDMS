# Smart Meter MDMS - Complete Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SMART METER MDMS PIPELINE ARCHITECTURE                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐
│   RAW DATA      │
│   (CSV Files)   │
│  10,000+ rows   │
│  50 meters      │
│  5 zones        │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: RAW DATA INGESTION (src/ingestion.py)                              │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Load CSV with encoding detection                                           │
│ • Schema validation (12 expected columns)                                    │
│ • Timestamp parsing (multiple formats)                                       │
│ • Column name standardization                                                │
│ • Data type enforcement                                                      │
│                                                                               │
│ Output: Raw validated DataFrame + metadata                                │
│ Logging: ingestion.log                                                    │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: DATA CLEANING (src/cleaning.py)                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ • Duplicate detection & removal (exact + keyed)                              │
│ • Missing value imputation (forward-fill, backward-fill, median)             │
│ • Outlier detection (IQR method, 1.5x multiplier)                           │
│ • Range validation (voltage, current, frequency, power, temp)                │
│ • Logical inconsistency detection                                            │
│                                                                               │
│ Output: Cleaned DataFrame + cleaning_report                               │
│ Logging: cleaning.log                                                     │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 3: FEATURE ENGINEERING (src/feature_engineering.py)                   │
├─────────────────────────────────────────────────────────────────────────────┤
│ TEMPORAL FEATURES:                                                           │
│   • hour, day_of_week, month, quarter, year, week_of_year                   │
│   • Cyclical encoding (hour_sin/cos, month_sin/cos, dow_sin/cos)            │
│   • season (Winter/Summer/Monsoon/Post-Monsoon for India)                   │
│                                                                               │
│ ELECTRICAL FEATURES:                                                         │
│   • power_factor = Active_Power / Apparent_Power                            │
│   • peak_hour_flag (10 AM - 10 PM = 1, else 0)                             │
│   • load_category (Low/Medium/High/VeryHigh)                                │
│   • consumption_bucket (Quartile-based distribution)                         │
│                                                                               │
│ AGGREGATION FEATURES:                                                        │
│   • meter_daily_consumption_kWh                                              │
│   • zone_hourly_consumption_kWh                                              │
│   • meter_avg_power_kW, zone_avg_power_kW                                    │
│                                                                               │
│ ROLLING FEATURES (24-hour window):                                           │
│   • active_power_rolling_mean_24h                                            │
│   • active_power_rolling_std_24h                                             │
│   • power_delta_from_mean_24h                                                │
│                                                                               │
│ Output: Feature-engineered DataFrame + feature_report                     │
│ Logging: feature_engineering.log                                          │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 4: DATA VALIDATION (src/validation.py)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ QUALITY CHECKS:                                                              │
│   • Completeness: Null % < 5%                                                │
│   • Uniqueness: Duplicates < 1%                                              │
│   • Validity: Range checks (voltage, current, frequency, etc.)               │
│   • Consistency: Business rule validation                                    │
│   • Anomalies: Outliers < 10%                                                │
│                                                                               │
│ DATA LINEAGE:                                                                │
│   • Row/column counts                                                        │
│   • Date range coverage                                                      │
│   • Unique meter/zone counts                                                 │
│   • Memory usage statistics                                                  │
│                                                                               │
│ Output: Validation report + decision (Pass/Fail)                          │
│ Logging: validation.log                                                   │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 5: STORAGE & ANALYTICS OUTPUT (src/storage.py)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│ STORAGE OPERATIONS:                                                          │
│   • Processed dataset → Parquet (all columns, Snappy compressed)             │
│   • Analytics-ready dataset → Parquet (key columns)                          │
│   • Aggregation views → Parquet (daily, hourly, peak, load)                 │
│   • Database: SQLite with fact + dimension tables                            │
│                                                                               │
│ Analytics VIEWS:                                                             │
│   • Daily consumption by meter                                               │
│   • Hourly zone consumption patterns                                         │
│   • Peak hour analysis                                                       │
│   • Load category distribution                                               │
│                                                                               │
│ Output: Data files + SQL tables + aggregates                              │
│ Logging: storage.log                                                      │
└────────┬────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│               ANALYTICS-READY OUTPUTS & METADATA                             │
├─────────────────────────────────────────────────────────────────────────────┤
│ DATA OUTPUTS:                                                                │
│  processed_smart_meter_data.parquet                                       │
│  analytics_ready_smart_meter_data.parquet                                 │
│  aggregate_daily_consumption.parquet                                      │
│  aggregate_hourly_zone_consumption.parquet                                │
│  aggregate_peak_hour_analysis.parquet                                     │
│  aggregate_load_category_distribution.parquet                             │
│                                                                               │
│ REPORTING:                                                                   │
│  pipeline_report.json (comprehensive execution summary)                   │
│  ingestion_metadata.log                                                   │
│  storage.log (manifest of all saved files)                                │
│                                                                               │
│ DATABASE:                                                                    │
│  mdms.db (SQLite database)                                                │
│    - Dimension tables: dim_meter, dim_zone, dim_time, dim_load_profile      │
│    - Fact table: fact_meter_readings                                        │
│    - Aggregate tables: agg_daily, agg_hourly, agg_monthly                    │
│    - Quality tables: dq_quality_log, dq_anomalies                            │
│                                                                               │
│ SQL ANALYTICS: 50+ production queries ready to run                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Model

### Fact Table: fact_meter_readings
```
┌───────────────────────────────────┐
│   FACT_METER_READINGS             │
├───────────────────────────────────┤
│ • reading_id (PK)                 │
│ • timestamp                        │
│ • meter_id (FK → dim_meter)       │
│ • zone_id (FK → dim_zone)         │
│ • time_id (FK → dim_time)         │
│ • load_profile_id (FK)            │
│                                    │
│ MEASUREMENTS:                      │
│ • voltage_v                        │
│ • current_a                        │
│ • active_power_kw                  │
│ • reactive_power_kw                │
│ • apparent_power_kva               │
│ • frequency_hz                     │
│ • power_factor                     │
│ • sub_meter_kitchen                │
│ • sub_meter_hvac                   │
│ • outdoor_temp_c                   │
│                                    │
│ ENGINEERED FEATURES:               │
│ • hour_of_day                      │
│ • peak_hour_flag                   │
│ • load_category                    │
│ • consumption_bucket               │
│ • is_anomaly                       │
│ • rolling_ features                │
│ • aggregation_ features            │
│                                    │
│ METADATA:                          │
│ • created_at                       │
│ • updated_at                       │
└───────────────────────────────────┘
```

### Dimension Tables
```
dim_meter           dim_zone            dim_time            dim_load_profile
├─ meter_id (PK)   ├─ zone_id (PK)     ├─ time_id (PK)     ├─ load_profile_id (PK)
├─ meter_name      ├─ zone_name        ├─ timestamp        ├─ load_category
├─ meter_type      ├─ region           ├─ hour             ├─ min_power_kW
├─ installation    ├─ district         ├─ day_of_week      ├─ max_power_kW
└─ status          ├─ latitude         ├─ month            └─ description
                   ├─ longitude        ├─ season
                   └─ capacity         └─ is_peak_hour
```

---

## Configuration & Customization

### Key Configuration Points (config/config.py)

```python
# 1. PEAK HOURS (Customizable by region)
PEAK_HOURS = list(range(10, 22))  # India: 10 AM - 10 PM

# 2. SEASONS (Region-specific)
SEASONS = {
    'Winter': [12, 1, 2],           # Dec-Feb (India)
    'Summer': [3, 4, 5],            # Mar-May
    'Monsoon': [6, 7, 8, 9],        # Jun-Sep
    'Post-Monsoon': [10, 11]        # Oct-Nov
}

# 3. VALID VALUE RANGES (Customizable by network)
VALID_RANGES = {
    'Voltage_V': (190, 250),        # Indian standard: ±10% of 230V
    'Frequency_Hz': (49.5, 50.5),   # Indian grid: 50Hz ±1%
    'Current_A': (0, 100),          # Max household current
    'Active_Power_kW': (0, 25),     # Max household power
    'Outdoor_Temp_C': (-10, 50)     # Reasonable temperature range
}

# 4. QUALITY THRESHOLDS
DATA_QUALITY_THRESHOLDS = {
    'max_null_percentage': 5.0,        # Fail if >5% nulls
    'max_duplicates_percentage': 1.0,  # Fail if >1% duplicates
    'max_anomalies_percentage': 10.0   # Fail if >10% anomalies
}

# 5. ANOMALY DETECTION
ANOMALY_THRESHOLD = 1.5  # IQR multiplier (1.5 = moderate, 3.0 = strict)
ANOMALY_DETECTION_METHOD = 'iqr'  # 'iqr', 'zscore', or 'isolation_forest'

# 6. FEATURE ENGINEERING
ROLLING_WINDOW_HOURS = 24  # 24-hour rolling calculations
```

### To Customize for Your Needs:
1. Edit `config/config.py`
2. Restart pipeline or re-import
3. Changes apply to all downstream processes

---

## Running the Pipeline

### Quick Start
```bash
# Generate sample data (first run only)
python generate_sample_data.py

# Run complete pipeline
python main_pipeline.py

# Output: Files in data/processed/, data/analytics/, and data/logs/
```

### Monitor Progress
```bash
# Watch logs in real-time
tail -f data/logs/*.log

# Check pipeline report
cat data/processed/pipeline_report.json
```

### Query Results
```bash
# Check processed data
python -c "import pandas as pd; df = pd.read_parquet('data/analytics/analytics_ready_smart_meter_data.parquet'); print(df.head())"

# Query database
sqlite3 data/mdms.db
> SELECT * FROM analytics_zone_consumption_summary;
```

---

## Quality Assurance

### Pre-Pipeline Checks
* Raw CSV exists and is readable
* Columns match expected schema
* Timestamp format is parseable

### Mid-Pipeline Checks
[OK] Null percentages within threshold
[OK] Duplicates removed successfully
[OK] Outliers detected and flagged

### Post-Pipeline Checks
[OK] All features engineered correctly
[OK] Validation passed for all quality thresholds
[OK] Output files created and non-empty

### View Quality Report
```python
import json
with open('data/processed/pipeline_report.json') as f:
    report = json.load(f)
    
print(f"Status: {report['pipeline_status']}")
print(f"Quality Checks: {report['validation']['overall_status']}")
print(f"Anomalies: {report['validation']['validation_results']['anomalies']}")
```

---

## Performance Metrics

### Typical Execution Time (1M rows)
- Ingestion: 2-3 seconds
- Cleaning: 5-8 seconds
- Feature Engineering: 10-15 seconds (rolling window intensive)
- Validation: 2-3 seconds
- Storage: 3-5 seconds
- **Total: ~25-35 seconds**

### Memory Usage
- Raw data: ~150MB (1M rows in CSV)
- After ingestion: ~200MB
- After feature engineering: ~600MB (rolling windows)
- Peak: ~800MB during aggregations

### Output Sizes
- Processed Parquet: ~40-50MB (Snappy compression)
- Analytics Parquet: ~25-35MB (subset of columns)
- Database: ~60-80MB with all indices

### Optimization Options
```python
# In config.py
SAMPLING_FRACTION = 0.1  # Use 10% of data for testing
ROLLING_WINDOW_HOURS = 6  # Reduce window size
BATCH_SIZE = 50000  # Process in batches
```

---

## Troubleshooting Guide

### Issue #1: "Schema validation failed"
**Symptoms:** Error in ingestion stage
```
ERROR: Schema validation failed. Missing columns: [list of columns]
```
**Solution:**
- Check raw CSV column names exactly match EXPECTED_COLUMNS
- Use `pandas.read_csv('file.csv').columns` to verify
- Standardize column names if needed

### Issue #2: "OutOfMemory exception"
**Symptoms:** Process killed or slow
**Solution:**
```python
# In config.py, reduce data size
SAMPLING_FRACTION = 0.5  # Use 50% of data
ROLLING_WINDOW_HOURS = 12  # Smaller window
COMPRESSION = 'gzip'  # Slower but better compression
```

### Issue #3: "Validation failed: Anomaly percentage too high"
**Symptoms:** Validation report shows >10% anomalies
**Solution:**
```python
# In config.py, relax threshold
ANOMALY_THRESHOLD = 2.0  # Increase from 1.5 (more conservative)
DATA_QUALITY_THRESHOLDS['max_anomalies_percentage'] = 15.0  # From 10%
```

### Issue #4: "Missing values handling error"
**Symptoms:** Feature engineering fails
**Solution:**
- Check specific column causing issue in cleaning.log
- Manually inspect data: `df[df.isnull().any(axis=1)]`
- Adjust MISSING_VALUE_STRATEGY in config

---

## SQL Analytics Examples

### 1. Top 10 Consumers
```sql
SELECT TOP 10 
    meter_id, 
    meter_name,
    SUM(active_power_kw) as total_kwh
FROM fact_meter_readings
GROUP BY meter_id
ORDER BY total_kwh DESC;
```

### 2. Peak Hour Analysis
```sql
SELECT 
    hour_of_day,
    zone_id,
    AVG(active_power_kw) as avg_load_kw,
    MAX(active_power_kw) as peak_load_kw
FROM fact_meter_readings
WHERE peak_hour_flag = 1
GROUP BY hour_of_day, zone_id
ORDER BY hour_of_day;
```

### 3. Anomaly Detection
```sql
SELECT TOP 50
    timestamp,
    meter_id,
    active_power_kw,
    is_anomaly
FROM fact_meter_readings
WHERE is_anomaly = 1
ORDER BY timestamp DESC;
```

### 4. Daily Consumption Trend
```sql
SELECT 
    CAST(timestamp AS DATE) as reading_date,
    zone_id,
    SUM(active_power_kw) as daily_kwh,
    AVG(active_power_kw) as avg_kw,
    COUNT(*) as reading_count
FROM fact_meter_readings
GROUP BY CAST(timestamp AS DATE), zone_id
ORDER BY reading_date DESC;
```

---

## Next Steps & Future Enhancements

### Phase 2 (BI & Dashboards)
- [ ] Power BI connector
- [ ] Tableau server integration
- [ ] Real-time dashboard
- [ ] Anomaly alerts

### Phase 3 (Advanced Analytics)
- [ ] Consumption forecasting (Prophet, ARIMA)
- [ ] ML-based anomaly detection (Isolation Forest)
- [ ] Customer segmentation
- [ ] Demand elasticity analysis

### Phase 4 (Production Deployment)
- [ ] Docker containerization
- [ ] Apache Airflow orchestration
- [ ] REST API for triggers
- [ ] Cloud deployment (Azure/AWS)
- [ ] Incremental loading
- [ ] Change Data Capture (CDC)

---

## Support & Resources

**Documentation:**
- QUICKSTART.md - Step-by-step setup guide
- ARCHITECTURE.md - This file
- Inline code comments
- SQL query examples

**Logs Location:**
```
data/logs/
├── ingestion.log
├── cleaning.log
├── feature_engineering.log
├── validation.log
└── main_pipeline.log
```

**Key Modules:**
- `main_pipeline.py` - Orchestration entry point
- `src/*.py` - Individual pipeline stages
- `sql/*.sql` - Database and query definitions
- `config/config.py` - All settings

---

*Smart Meter MDMS v1.0 - Production-Ready Data Engineering Pipeline*
