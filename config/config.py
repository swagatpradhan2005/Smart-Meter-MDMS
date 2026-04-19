"""
Configuration module for Smart Meter MDMS Pipeline.
Centralized settings for data paths, validation rules, and processing parameters.
"""

import os
from pathlib import Path
from datetime import datetime

# Project Paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
ANALYTICS_DATA_DIR = DATA_DIR / "analytics"
LOG_DIR = DATA_DIR / "logs"
SQL_DIR = PROJECT_ROOT / "sql"

# Create directories if they don't exist
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, ANALYTICS_DATA_DIR, LOG_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Data Schema Configuration
EXPECTED_COLUMNS = [
    'Timestamp',
    'Meter_ID',
    'Zone_ID',
    'Voltage_V',
    'Current_A',
    'Active_Power_kW',
    'Reactive_Power_kW',
    'Apparent_Power_kVA',
    'Frequency_Hz',
    'Sub_Meter_Kitchen',
    'Sub_Meter_HVAC',
    'Outdoor_Temp_C'
]

# Data Type Mappings
DATA_TYPES = {
    'Timestamp': 'datetime64[ns]',
    'Meter_ID': 'string',
    'Zone_ID': 'string',
    'Voltage_V': 'float32',
    'Current_A': 'float32',
    'Active_Power_kW': 'float32',
    'Reactive_Power_kW': 'float32',
    'Apparent_Power_kVA': 'float32',
    'Frequency_Hz': 'float32',
    'Sub_Meter_Kitchen': 'float32',
    'Sub_Meter_HVAC': 'float32',
    'Outdoor_Temp_C': 'float32'
}

# Validation Thresholds
VALID_RANGES = {
    'Voltage_V': (190, 250),          # Typical Indian supply: 190-250V
    'Current_A': (0, 100),             # Typical household: 0-100A
    'Active_Power_kW': (0, 25),        # Typical household: 0-25kW
    'Reactive_Power_kW': (0, 25),
    'Apparent_Power_kVA': (0, 40),
    'Frequency_Hz': (49.5, 50.5),      # Indian grid: 50Hz +/- 0.5Hz
    'Outdoor_Temp_C': (-10, 50),       # Reasonable temperature range
    'Sub_Meter_Kitchen': (0, 5),       # Sub-meter ranges
    'Sub_Meter_HVAC': (0, 10)
}

# Missing Value Handling
MISSING_VALUE_STRATEGY = {
    'numeric': 'forward_fill',  # forward fill, then backward fill, then median
    'categorical': 'mode'
}

# Feature Engineering Parameters
ROLLING_WINDOW_HOURS = 24
ANOMALY_DETECTION_METHOD = 'iqr'  # 'iqr', 'zscore', or 'isolation_forest'
ANOMALY_THRESHOLD = 1.5  # IQR multiplier for outlier detection

# Peak Hour Definition (24-hour format)
PEAK_HOURS = list(range(10, 22))  # 10 AM to 10 PM

# Seasons (Indian context)
SEASONS = {
    'Winter': [12, 1, 2],      # Dec, Jan, Feb
    'Summer': [3, 4, 5],       # Mar, Apr, May
    'Monsoon': [6, 7, 8, 9],   # Jun-Sep
    'Post-Monsoon': [10, 11]   # Oct, Nov
}

# Load Categories
LOAD_CATEGORIES = {
    'Low': (0, 1.0),
    'Medium': (1.0, 3.0),
    'High': (3.0, 10.0),
    'VeryHigh': (10.0, float('inf'))
}

# Logging
LOG_FORMAT = '%(asctime)s | %(name)s | %(levelname)s | %(message)s'
LOG_LEVEL = 'INFO'

# Database Configuration (for future use)
DATABASE_CONFIG = {
    'driver': 'sqlite',  # Will support PostgreSQL later
    'database': str(PROJECT_ROOT / 'data' / 'mdms.db'),
    'timeout': 30
}

# Quality Check Thresholds
DATA_QUALITY_THRESHOLDS = {
    'max_null_percentage': 5.0,        # Max 5% nulls per column
    'max_duplicates_percentage': 1.0,  # Max 1% duplicates
    'max_anomalies_percentage': 10.0   # Max 10% anomalies
}

# Processing Configuration
BATCH_SIZE = 10000  # For large file processing
SAMPLING_FRACTION = 1.0  # Set < 1.0 for testing on sample data

# Output Formats
OUTPUT_FORMAT = 'parquet'  # 'parquet' or 'csv'
COMPRESSION = 'snappy'
