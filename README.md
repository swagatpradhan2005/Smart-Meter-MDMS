# ⚡ Smart Meter MDMS (Meter Data Management System)

🚀 **End-to-End Data Engineering Pipeline for Smart Meter Analytics**

> A structured end-to-end data engineering pipeline that transforms raw smart meter data into cleaned datasets, analytical outputs, and an interactive dashboard for insight generation.

---

## 📌 Problem Statement

Smart meter data is generated continuously in large volumes, often in raw and inconsistent formats. This makes it difficult for utilities to directly use the data for monitoring, decision-making, and performance analysis.

The challenge lies in building a system that can reliably ingest, clean, validate, and transform this data into meaningful insights. Without such a pipeline, issues like missing values, duplicate records, and anomalies reduce data quality and limit analytical usability.

This project addresses these challenges by designing a modular data engineering pipeline that processes raw meter readings into structured datasets, generates analytical outputs, and presents insights through a dashboard for easy interpretation.

---

## 📸 Dashboard Preview

### 🔹 Overview
<img width="1600" height="754" alt="image" src="https://github.com/user-attachments/assets/3e421a8b-66d8-4e21-8bad-dd82e737238e" />

### 🔹 Trends
<img width="1600" height="764" alt="image" src="https://github.com/user-attachments/assets/a1d61200-87ad-4659-b513-f492e3ddca2f" />

### 🔹 Visual Plots
<img width="1600" height="761" alt="image" src="https://github.com/user-attachments/assets/8c3de370-cb7e-4eeb-a0df-f635ec7e04b2" />

---

## ⚙️ Features

- ✔ Data ingestion from raw CSV datasets  
- ✔ Data cleaning (handling duplicates, missing values, invalid values)  
- ✔ Feature engineering (temporal, load-based, rolling metrics)  
- ✔ Data validation using basic quality checks  
- ✔ Multi-format storage (CSV, Parquet, SQLite)  
- ✔ SQL-based analytics outputs  
- ✔ Interactive dashboard using Streamlit  
- ✔ Modular pipeline structure  

---

## 🏗️ Project Structure

```
Smart Meter Data Systems/
│
├── src/                      # Core processing modules
├── pipeline/                 # Spark, Kafka, orchestration layers
├── sql/                      # SQL queries and schema
├── data/
│   ├── raw/
│   ├── processed/
│   ├── curated/
│   └── analytics/
│
├── outputs/
│   ├── plots/
│   └── reports/
│
├── notebooks/                # Step-by-step analysis notebooks
├── dashboard.py              # Streamlit dashboard
├── run_all.py                # Main pipeline runner
└── README.md
```

---

## 🔄 Pipeline Overview

```
Raw Data
↓
Ingestion
↓
Cleaning
↓
Feature Engineering
↓
Validation
↓
Storage
↓
SQL Analytics
↓
EDA & Reports
↓
Dashboard
```

---

## ▶️ Quick Start

Run full pipeline:

```bash
python run_all.py
```

Run dashboard:

```bash
streamlit run dashboard.py
```

---

## 📊 Key Outputs

* Cleaned dataset (`data/processed/`)
* Feature dataset (`data/curated/`)
* Analytics CSV outputs (`data/analytics/`)
* Visual plots (`outputs/plots/`)
* SQL summary reports (`outputs/reports/`)
* Interactive dashboard

---

## 🧠 Key Insights

* Peak vs off-peak consumption patterns
* Zone-wise performance comparison
* Daily and hourly consumption trends
* Load variability analysis
* Basic anomaly identification

---

## 🛠️ Tech Stack

* **Language:** Python
* **Libraries:** Pandas, NumPy
* **Big Data (simulated):** PySpark
* **Streaming (simulated):** Kafka
* **Storage:** CSV, Parquet, SQLite
* **Visualization:** Matplotlib, Seaborn
* **Dashboard:** Streamlit
* **Orchestration:** Airflow (DAG definitions)

---

## ⭐ Highlights

* Organized and modular pipeline design
* Clear separation of ingestion, processing, and analytics
* Combination of batch processing and simulated streaming
* SQL-based analytics layer for reusable outputs
* Dashboard combining charts, tables, and summaries

---

## 🚀 Future Improvements

* Add real-time data streaming support
* Enhance dashboard interactivity (filters, drill-downs)
* Improve anomaly detection techniques
* Deploy pipeline on cloud platforms
* Integrate automated scheduling

---

## 👤 Author

**Swagat Pradhan**
CSE, B.Tech
KIIT University

GitHub: [https://github.com/swagatpradhan2005](https://github.com/swagatpradhan2005)

---

## 📄 License

This project is licensed under the MIT License.

---
