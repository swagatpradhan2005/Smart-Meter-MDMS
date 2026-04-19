# Smart Meter Data Management System (MDMS) - Quick Start Guide

## Project Overview
This is a production-grade data engineering pipeline for smart meter data processing, designed for Indian utilities but scalable globally. The system handles raw meter data ingestion, comprehensive cleaning, intelligent feature engineering, validation, and analytics-ready output generation.

### Core Features
* Raw data ingestion with schema validation
* Comprehensive data cleaning (missing values, outliers, duplicates)
* Feature engineering (temporal, electrical, aggregation features)
* Data quality validation and anomaly detection
* Structured relational database design
* Analytics-ready outputs and views
* Production logging and error handling

---

## Project Structure

```
Smart Meter Data Systems/
├── data/
│   ├── raw/                          # Raw input data (CSV files)
│   ├── processed/                    # Cleaned, feature-engineered data (Parquet)
│   ├── analytics/                    # Analytics-optimized outputs
│   └── logs/                         # Pipeline execution logs
│
├── src/                              # Core pipeline modules
│   ├── __init__.py
│   ├── config.py                     # Configuration and settings
│   ├── utils.py                      # Utility functions and validation
│   ├── ingestion.py                  # Raw data loading
│   ├── cleaning.py                   # Data cleaning operations
│   ├── feature_engineering.py        # Feature creation
│   ├── validation.py                 # Data quality checks
│   └── storage.py                    # Database/file persistence
│
├── sql/                              # Database schema and queries
│   ├── schema.sql                    # Relational DB schema (fact + dimensions)
│   ├── analytics_queries.sql         # 50+ production analytics queries
│   └── data_quality_checks.sql       # Quality check queries
│
├── config/                           # Configuration files
│   └── config.py                     # All pipeline settings
│
├── main_pipeline.py                  # Main orchestrator script
├── generate_sample_data.py           # Sample data generator
├── requirements.txt                  # Python dependencies
└── QUICKSTART.md                     # This file

```

---

## Installation & Setup

### 1. Install Python Dependencies
```bash
pip install -r requirements.txt
```

Required packages:
- pandas, numpy: Data manipulation
- pyarrow: Parquet support
- scipy, scikit-learn: Analytics/ML features
- SQLAlchemy: Database abstraction

### 2. Generate Sample Data (First Run Only)
```bash
python generate_sample_data.py
```

This creates realistic test data with:
- 10,000+ smart meter readings
- 50 unique meters across 5 zones
- 3-month date range
- Realistic data quality issues (2% missing, 1% outliers, 0.5% duplicates)
- Seasonal and diurnal patterns

### 3. Run Complete Pipeline
```bash
python main_pipeline.py
```

The pipeline will:
1. Load and validate raw data
2. Clean missing values, handle duplicates, flag outliers
3. Engineer temporal (hour, season, peak_hour), electrical (power_factor), and aggregation features
4. Validate data quality against thresholds
5. Save processed data to Parquet format
6. Generate analytics-ready outputs with aggregation views
7. Create comprehensive pipeline report

---

## Pipeline Stages

### Stage 1: Raw Data Ingestion (`ingestion.py`)
**Input:** CSV file with meter readings
**Operations:**
- Load CSV with encoding detection
- Validate schema against expected columns
- Parse timestamps (multiple formats supported)
- Standardize column names
- Enforce data types

**Output:** Ingestion metadata, quality report

**Example:**
```python
from src.ingestion import RawDataIngestion

ingestion = RawDataIngestion()
df, metadata = ingestion.ingest_and_prepare('data/raw/sample_raw_data.csv')
```

---

### Stage 2: Data Cleaning (`cleaning.py`)
**Input:** Raw DataFrame
**Operations:**
- Remove duplicate rows (exact and timestamp-meter combos)
- Handle missing values (forward-fill, backward-fill, median imputation)
- Detect outliers using IQR method (1.5x multiplier)
- Validate value ranges (voltage, current, frequency, power)
- Detect logical inconsistencies (Apparent Power = sqrt(Active² + Reactive²))

**Output:** Cleaned DataFrame, cleaning report with row counts

**Example:**
```python
from src.cleaning import DataCleaner

cleaner = DataCleaner()
df_cleaned, report = cleaner.clean_pipeline(df_raw)
```

---

### Stage 3: Feature Engineering (`feature_engineering.py`)
**Input:** Cleaned DataFrame
**Features Created:**

**Temporal Features:**
- hour, day_of_week, month, quarter, year, week_of_year
- Cyclical encoding (hour_sin, hour_cos, etc.)
- season (Winter/Summer/Monsoon/Post-Monsoon for India)
- day_name

**Electrical Features:**
- power_factor = Active_Power / Apparent_Power
- peak_hour_flag (10 AM - 10 PM = 1, else 0)
- load_category (Low/Medium/High/VeryHigh)
- consumption_bucket (Quartile-based)

**Aggregation Features:**
- meter_daily_consumption_kWh
- zone_hourly_consumption_kWh
- meter_avg_power_kW
- zone_avg_power_kW

**Rolling Features (24-hour window):**
- active_power_rolling_mean_24h
- active_power_rolling_std_24h
- power_delta_from_mean_24h

**Output:** Feature-engineered DataFrame, feature report

**Example:**
```python
from src.feature_engineering import FeatureEngineer

engineer = FeatureEngineer()
df_engineered, feature_report = engineer.feature_engineering_pipeline(df_cleaned)
```

---

### Stage 4: Data Validation (`validation.py`)
**Input:** Feature-engineered DataFrame
**Quality Checks:**
- **Completeness:** Null percentage by column (threshold: 5%)
- **Uniqueness:** Duplicate detection (threshold: 1%)
- **Validity:** Range checks (voltage 190-250V, frequency 49.5-50.5Hz, etc.)
- **Consistency:** Logical business rules, timestamp continuity
- **Anomalies:** Outlier percentage (threshold: 10%)

**Output:** Validation passed/failed, comprehensive report

**Example:**
```python
from src.validation import DataValidator

validator = DataValidator()
passed, report = validator.validation_pipeline(df_engineered)
```

---

### Stage 5: Storage & Analytics Output (`storage.py`)
**Input:** Validated, feature-engineered DataFrame
**Operations:**
- Save processed data (all columns) → Parquet (Snappy compressed)
- Build analytics-ready dataset (selected key columns)
- Create aggregation views:
  - Daily consumption by meter
  - Hourly zone consumption
  - Peak hour analysis
  - Load category distribution
- Save all outputs with metadata

**Output Datasets:**
- `processed_smart_meter_data.parquet` (~data/processed/)
- `analytics_ready_smart_meter_data.parquet` (~data/analytics/)
- `aggregate_*.parquet` views (~data/analytics/)
- Pipeline report in JSON (~data/processed/)

**Example:**
```python
from src.storage import DataStorage

storage = DataStorage()
processed_file = storage.save_to_parquet(df, 'my_dataset', directory='processed')
```

---

## Configuration

All settings in `config/config.py`:

```python
# Peak Hours (Indian context)
PEAK_HOURS = list(range(10, 22))  # 10 AM to 10 PM

# Seasons
SEASONS = {
    'Winter': [12, 1, 2],
    'Summer': [3, 4, 5],
    'Monsoon': [6, 7, 8, 9],
    'Post-Monsoon': [10, 11]
}

# Valid Ranges
VALID_RANGES = {
    'Voltage_V': (190, 250),
    'Frequency_Hz': (49.5, 50.5),
    'Current_A': (0, 100),
    ...
}

# Quality Thresholds
DATA_QUALITY_THRESHOLDS = {
    'max_null_percentage': 5.0,
    'max_duplicates_percentage': 1.0,
    'max_anomalies_percentage': 10.0
}
```

Modify these based on your business rules.

---

## Database Schema

SQLite schema with fact and dimension tables:

### Dimension Tables
- `dim_time` - Time dimension (date, hour, season, peak_flag)
- `dim_meter` - Meter master data
- `dim_zone` - Zone/region information
- `dim_load_profile` - Load categories

### Fact Table
- `fact_meter_readings` - Main readings (1.7M+ rows possible)

### Aggregate Tables (for reporting)
- `agg_daily_consumption` - Optimized daily queries
- `agg_hourly_zone_consumption` - Zone-level hourly
- `agg_monthly_consumption` - Monthly summaries

### Quality Tables
- `dq_quality_log` - QA check entries
- `dq_anomalies` - Flagged anomaly records

To initialize schema:
```sql
-- From command line:
sqlite3 data/mdms.db < sql/schema.sql

-- Or programmatically:
import sqlite3
conn = sqlite3.connect('data/mdms.db')
with open('sql/schema.sql', 'r') as f:
    conn.executescript(f.read())
```

---

## Analytics Queries

50+ production-ready SQL queries in `sql/analytics_queries.sql`:

### Consumption Analytics
```sql
-- Daily consumption by meter
SELECT * FROM analytics_daily_consumption_by_meter;

-- Monthly trends
SELECT * FROM analytics_monthly_consumption_trend;

-- Top 10 consumers
SELECT TOP 10 * FROM analytics_zone_consumption_summary;
```

### Peak Hours
```sql
-- Peak vs off-peak breakdown
SELECT period_type, SUM(total_kwh) FROM peak_vs_offpeak GROUP BY period_type;

-- Hourly load profile
SELECT hour_of_day, AVG(load_kw) FROM hourly_load_profile GROUP BY hour_of_day;
```

### Data Quality
```sql
-- Anomaly summary
SELECT * FROM analytics_anomalies_by_meter;

-- Quality metrics
SELECT * FROM dq_quality_log ORDER BY check_timestamp DESC;
```

---

## Data Quality Checks

Comprehensive validation queries in `sql/data_quality_checks.sql`:

### Before Running Pipeline
- Check null percentages per column
- Identify meter data gaps
- Detect duplicates

### After Running Pipeline
- Validate date ranges
- Check anomaly percentages
- Verify calculated fields
- Monitor quality score over time

### View Quality Report
```sql
-- Overall quality score
SELECT * FROM dq_quality_summary;

-- Time-series trend
SELECT reading_date, daily_quality_score FROM dq_quality_trend ORDER BY reading_date DESC;
```

---

## Output Files

### Processed Dataset
`data/processed/processed_smart_meter_data.parquet`
- All engineered features
- Complete metadata
- Ready for ML model training

### Analytics Dataset
`data/analytics/analytics_ready_smart_meter_data.parquet`
- Selected key columns
- Optimized for BI queries
- Reduced file size

### Aggregation Views
```
data/analytics/
├── aggregate_daily_consumption.parquet
├── aggregate_hourly_zone_consumption.parquet
├── aggregate_peak_hour_analysis.parquet
└── aggregate_load_category_distribution.parquet
```

### Logs & Reports
```
data/logs/
├── ingestion.log              # Raw data loading
├── cleaning.log               # Cleaning operations
├── feature_engineering.log    # Feature creation
├── validation.log             # QA checks
└── storage.log                # File operations

data/processed/
├── pipeline_report.json       # Complete pipeline execution report
├── storage.log                # Storage manifest
└── ingestion_metadata.log     # Raw data metadata
```

---

## Python API Usage

### Complete Pipeline Example
```python
from main_pipeline import SmartMeterPipeline
from config.config import RAW_DATA_DIR

# Run full pipeline
pipeline = SmartMeterPipeline()
report = pipeline.run_complete_pipeline(str(RAW_DATA_DIR / 'raw_data.csv'))

# Print summary
print(f"Status: {report['pipeline_status']}")
print(f"Final rows: {report['final_dataset']['rows']}")
print(f"Duration: {report['duration_seconds']:.2f}s")
```

### Individual Stage Usage
```python
import pandas as pd
from src.ingestion import RawDataIngestion
from src.cleaning import DataCleaner
from src.feature_engineering import FeatureEngineer

# Step 1: Ingest
ingestion = RawDataIngestion()
df, meta = ingestion.ingest_and_prepare('data/raw/data.csv')

# Step 2: Clean
cleaner = DataCleaner()
df_clean, clean_report = cleaner.clean_pipeline(df)

# Step 3: Feature Engineer
engineer = FeatureEngineer()
df_engineered, feature_report = engineer.feature_engineering_pipeline(df_clean)
```

### Custom Validation
```python
from src.validation import DataValidator

validator = DataValidator()
passed, report = validator.validation_pipeline(df_engineered)

if not passed:
    print(f"Validation failed: {report['overall_status']}")
    print(report['validation_results'])
```

---

## Performance & Scalability

### Current Dataset Size
- **Sampler data:** 10,000-1M rows
- **Processing time:** <5 seconds (1M rows)
- **Memory usage:** ~500MB for 1M rows
- **Disk output:** ~50-100MB compressed Parquet

### Optimization Tips
1. **Sampling:** Set `SAMPLING_FRACTION < 1.0` in config for rapid testing
2. **Batch processing:** Use `BATCH_SIZE` parameter for large files
3. **Compression:** Parquet with Snappy compression (default)
4. **Indexing:** SQL indexes on common query columns

### Scaling for Production
- PostgreSQL instead of SQLite
- Partitioning by date/zone
- Incremental ingestion (daily delta loads)
- Parallel processing for multiple zones
- Cloud storage (S3, Azure Blob)

---

## Troubleshooting

### Issue: "Raw data file not found"
```
Solution: Run generate_sample_data.py first
```

### Issue: "Schema validation failed"
```
Solution: Check column names match EXPECTED_COLUMNS in config.py
Ensure timestamp format is parseable (see parse_timestamp_safe())
```

### Issue: "OutOfMemory"
```
Solution: 
- Reduce SAMPLING_FRACTION in config
- Process files in batches
- Use BATCH_SIZE parameter
```

### Issue: "Duplicate handling too aggressive"
```
Solution: Adjust duplicate detection logic in cleaning.py
Change subset parameter in df.drop_duplicates()
```

---

## Next Steps (Not Included in v1.0)

**Phase 2: BI & Dashboards**
- Power BI / Tableau connectors
- Real-time dashboards
- Alerting on anomalies
- Custom reports

**Phase 3: Advanced Analytics**
- Consumption forecasting (ARIMA, Prophet)
- Anomaly ML models (Isolation Forest, LSTM)
- Clustering (High/Low usage patterns)
- Demand forecasting

**Phase 4: Production Deployment**
- Docker containerization
- Airflow DAG orchestration
- REST API for pipeline triggers
- Cloud deployment (Azure/AWS/GCP)

---

## Support & Documentation

**Key Files:**
- `main_pipeline.py` - Full pipeline orchestration
- `sql/analytics_queries.sql` - 50+ example analytics queries
- `config/config.py` - All configuration options
- `src/utils.py` - Common utility functions

**Logging:**
All operations logged to `data/logs/` - check specific module logs for detailed diagnostics.

---

## License & Attribution
© 2024 Smart Meter MDMS Project
Production-ready data engineering pipeline for utility grid analytics
