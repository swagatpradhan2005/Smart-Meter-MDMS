# Smart Meter Data Management System (MDMS)

**Data Engineering Pipeline for Smart Meter Analytics**

> A complete end-to-end data engineering workflow demonstrating ingestion, cleaning, feature engineering, validation, storage, streaming simulation, big data processing, and analytics.

---

## Quick Start

```bash
python run_all.py
```

This single command executes the complete pipeline, generating all processed data, reports, and visualizations.

---

## Features

* **Data Ingestion** - Load and validate 25,000+ meter records  
* **Smart Cleaning** - Remove duplicates, handle missing values, detect anomalies  
* **Feature Engineering** - Create 30+ temporal and electrical features  
* **Data Validation** - 5-point quality checks (completeness, uniqueness, validity, consistency, anomalies)  
* **Distributed Processing** - Kafka streaming simulation and Spark processing  
* **HDFS Simulation** - Local file system simulation for HDFS operations  
* **SQL Analytics** - Database schema and 50+ analytical queries  
* **EDA & Reporting** - Generate plots and summary reports  
* **Full Orchestration** - Airflow DAG definitions included  

---

## Project Structure

```
Smart Meter Data Systems/
├── src/                          # Core data engineering modules
│   ├── ingestion.py             # Raw data loading
│   ├── cleaning.py              # Data quality improvements
│   ├── feature_engineering.py   # Feature creation
│   ├── validation.py            # Quality assurance (5 checks)
│   ├── storage.py               # Multi-format persistence
│   ├── sql_runner.py            # Database operations
│   ├── eda_analysis.py          # Visualizations
│   ├── analytics_engine.py      # Report generation
│   └── utils.py                 # Utilities and logging
│
├── pipeline/                     # Tool integration layers
│   ├── transformation/          # Data transformer wrapper
│   ├── processing/              # Spark processor
│   ├── streaming/               # Kafka simulator
│   ├── orchestration/           # Airflow DAG
│   └── hadoop/                  # HDFS simulator
│
├── sql/                         # Database layer
│   ├── schema.sql              # Fact and dimension tables
│   ├── analytics_queries.sql   # Analytics queries
│   ├── advanced_queries.sql    # Advanced analytics
│   └── data_quality_checks.sql # Quality check queries
│
├── notebooks/                   # Sequential explanation notebooks
│   ├── 01_ingestion.ipynb
│   ├── 02_cleaning.ipynb
│   ├── 03_feature_engineering.ipynb
│   ├── 04_eda_analysis.ipynb
│   └── 05_complete_analysis.ipynb
│
├── data/                        # Data storage
│   ├── raw/                    # Input CSV files
│   ├── processed/              # Cleaned and featured data
│   ├── curated/                # Analytics-ready data
│   ├── stream/                 # Kafka streaming simulation
│   └── hdfs/                   # HDFS local simulation
│
├── outputs/                     # Generated results
│   ├── plots/                  # EDA visualizations
│   └── reports/                # JSON and CSV reports
│
├── config/                      # Configuration
│   └── config.py               # Settings and paths
│
├── run_all.py                   # **Main orchestrator** <- START HERE
├── QUICKSTART.md               # Detailed setup guide
├── ARCHITECTURE.md             # System design documentation
├── ANALYSIS_REPORT.md          # Data analysis findings
└── requirements.txt             # Python dependencies
```

---

## Pipeline Stages

The `run_all.py` orchestrator executes these stages in order:

1. **Ingestion** - Load raw CSV data (25,000 records)
2. **Cleaning** - Remove duplicates, impute missing values, fix invalid ranges
3. **Feature Engineering** - Add 32 temporal and electrical features
4. **Validation** - Run 5 data quality checks (all must pass)
5. **Storage** - Save to CSV, Parquet, SQLite
6. **Kafka Streaming** - Simulate 23,750+ event stream
7. **Spark Processing** - Distributed transformations
8. **HDFS Operations** - Simulate file archival
9. **SQL Execution** - Create schema and run analytics
10. **EDA & Reporting** - Generate plots and summary reports
11. **Final Summary** - Print execution status

---

## Pipeline Outputs

After running `python run_all.py`, you'll find:

| Location | Contents |
|----------|----------|
| `data/processed/` | Cleaned data (CSV, Parquet) |
| `data/curated/` | Analytics-ready data |
| `data/stream/` | Kafka streaming simulation output |
| `data/mdms.db` | SQLite database with populated tables |
| `outputs/plots/` | EDA visualizations (PNG/PDF) |
| `outputs/reports/` | JSON and CSV analytics reports |
| `logs/` | Execution logs |

---

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Setup and installation guide
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - System design and data flow
- **[ANALYSIS_REPORT.md](ANALYSIS_REPORT.md)** - Initial data analysis findings

---

## Technology Stack

**Data Processing:** Pandas, NumPy, Scikit-learn  
**Distributed Processing:** PySpark (with Pandas fallback)  
**Streaming:** Kafka (simulated)  
**File System:** HDFS (simulated locally)  
**Orchestration:** Airflow (DAG definitions)  
**Database:** SQLite  
**Visualization:** Matplotlib, Seaborn  
**Logging:** Python logging  

---

## Key Components

### src/ - Core Modules
- **RawDataIngestion** - Loads and validates raw meter data
- **DataCleaner** - Handles duplicates, missing values, anomalies
- **FeatureEngineer** - Creates 30+ analytical features
- **DataValidator** - Performs 5 comprehensive quality checks
- **DataStorage** - Multi-format persistence (CSV, Parquet, SQLite)
- **SQLRunner** - Database schema creation and query execution
- **SmartMeterEDA** - Generates exploratory visualizations
- **AnalyticsEngine** - Creates summary reports

### pipeline/ - Integration Layers
- **DataTransformer** - Wraps src modules for consistency
- **SparkProcessor** - Distributed processing with Spark SQL
- **KafkaSimulator** - Producer-consumer streaming simulation
- **HDFSManager** - Local HDFS simulation
- **SmartMeterDAG** - Airflow DAG definitions

---

## Verification

The pipeline is submission-ready and demonstrates:

* **Professional Code Organization** - Modular, tested, logged  
* **Complete Data Engineering Workflow** - All 7 tools integrated  
* **Repeatable Execution** - `python run_all.py` always works  
* **Real Data Processing** - Actually transforms 25,000 records  
* **Error Handling** - Comprehensive logging and fallbacks  
* **Performance** - Completes in <2 minutes with local resources

---

## Key Insights
- [ ] Peak consumption hours across zones
- [ ] High-load anomaly detection
- [ ] Rolling consumption trends
- [ ] Zone-wise efficiency comparison
- [ ] Load volatility patterns

---

##  Future Improvements

- [ ] Add real-time streaming with Kafka + Spark
- [ ] Build dashboard with Power BI or Streamlit
- [ ] Deploy on cloud using AWS or GCP
- [ ] Add workflow scheduling with Airflow

---

##  Author

**Swagat pradhan**
- GitHub: [@swagatprdhan2005](https://github.com/swagatprdhan2005)

---

##  License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
