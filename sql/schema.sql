-- Smart Meter MDMS Database Schema
-- Relational database design for smart meter data management and analytics
-- Supports fact and dimension tables for efficient querying

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Time Dimension Table
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER PRIMARY KEY,
    timestamp DATETIME UNIQUE NOT NULL,
    hour INTEGER,
    day_of_week INTEGER,
    day_of_month INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    week_of_year INTEGER,
    day_name TEXT,
    is_peak_hour INTEGER DEFAULT 0,
    season TEXT
);

-- Meter Dimension Table
CREATE TABLE IF NOT EXISTS dim_meter (
    meter_id TEXT PRIMARY KEY,
    meter_name TEXT,
    meter_type TEXT DEFAULT 'Smart Meter',
    installation_date DATE,
    status TEXT DEFAULT 'Active',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Zone Dimension Table
CREATE TABLE IF NOT EXISTS dim_zone (
    zone_id TEXT PRIMARY KEY,
    zone_name TEXT UNIQUE NOT NULL,
    region TEXT,
    district TEXT,
    latitude REAL,
    longitude REAL,
    supply_voltage REAL,
    transformer_capacity_kVA REAL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Load Profile Dimension Table
CREATE TABLE IF NOT EXISTS dim_load_profile (
    load_profile_id INTEGER PRIMARY KEY AUTOINCREMENT,
    load_category TEXT UNIQUE NOT NULL,
    min_power_kW REAL,
    max_power_kW REAL,
    typical_profile TEXT,
    description TEXT
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- Meter Readings Fact Table (Main fact table)
CREATE TABLE IF NOT EXISTS fact_meter_readings (
    reading_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME NOT NULL,
    meter_id TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    time_id INTEGER,
    load_profile_id INTEGER,
    
    -- Electrical measurements
    voltage_v REAL,
    current_a REAL,
    active_power_kw REAL,
    reactive_power_kw REAL,
    apparent_power_kva REAL,
    frequency_hz REAL,
    power_factor REAL,
    
    -- Sub-meter readings
    sub_meter_kitchen REAL,
    sub_meter_hvac REAL,
    
    -- Environmental
    outdoor_temp_c REAL,
    
    -- Engineered features
    hour_of_day INTEGER,
    peak_hour_flag INTEGER,
    load_category TEXT,
    consumption_bucket TEXT,
    
    -- Data quality flags
    is_anomaly INTEGER DEFAULT 0,
    is_anomaly_raw INTEGER DEFAULT 0,
    
    -- Rolling features (sample)
    active_power_rolling_mean_24h REAL,
    active_power_rolling_std_24h REAL,
    power_delta_from_mean_24h REAL,
    
    -- Aggregates
    meter_daily_consumption_kwh REAL,
    zone_hourly_consumption_kwh REAL,
    meter_avg_power_kw REAL,
    zone_avg_power_kw REAL,
    
    -- Metadata
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (meter_id) REFERENCES dim_meter(meter_id),
    FOREIGN KEY (zone_id) REFERENCES dim_zone(zone_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (load_profile_id) REFERENCES dim_load_profile(load_profile_id)
);

-- Create indexes for query optimization
CREATE INDEX idx_readings_timestamp ON fact_meter_readings(timestamp);
CREATE INDEX idx_readings_meter_id ON fact_meter_readings(meter_id);
CREATE INDEX idx_readings_zone_id ON fact_meter_readings(zone_id);
CREATE INDEX idx_readings_time_id ON fact_meter_readings(time_id);
CREATE INDEX idx_readings_peak_hour ON fact_meter_readings(peak_hour_flag);
CREATE INDEX idx_readings_anomaly ON fact_meter_readings(is_anomaly);
CREATE INDEX idx_readings_meter_timestamp ON fact_meter_readings(meter_id, timestamp);
CREATE INDEX idx_readings_zone_timestamp ON fact_meter_readings(zone_id, timestamp);

-- ============================================================================
-- AGGREGATE TABLES FOR FAST REPORTING
-- ============================================================================

-- Daily Consumption Aggregate (optimized for daily queries)
CREATE TABLE IF NOT EXISTS agg_daily_consumption (
    agg_id INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_date DATE NOT NULL,
    meter_id TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    
    total_consumption_kwh REAL,
    avg_power_kw REAL,
    max_power_kw REAL,
    min_power_kw REAL,
    
    peak_consumption_kwh REAL,
    off_peak_consumption_kwh REAL,
    
    avg_power_factor REAL,
    avg_frequency_hz REAL,
    
    anomaly_count INTEGER DEFAULT 0,
    reading_count INTEGER,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (meter_id) REFERENCES dim_meter(meter_id),
    FOREIGN KEY (zone_id) REFERENCES dim_zone(zone_id)
);

CREATE INDEX idx_agg_daily_date ON agg_daily_consumption(reading_date);
CREATE INDEX idx_agg_daily_meter ON agg_daily_consumption(meter_id);
CREATE INDEX idx_agg_daily_zone ON agg_daily_consumption(zone_id);

-- Hourly Zone Consumption Aggregate
CREATE TABLE IF NOT EXISTS agg_hourly_zone_consumption (
    agg_id INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_hour DATETIME NOT NULL,
    zone_id TEXT NOT NULL,
    
    total_consumption_kwh REAL,
    avg_power_kw REAL,
    max_power_kw REAL,
    min_power_kw REAL,
    meter_count INTEGER,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (zone_id) REFERENCES dim_zone(zone_id)
);

CREATE INDEX idx_agg_hourly_hour ON agg_hourly_zone_consumption(reading_hour);
CREATE INDEX idx_agg_hourly_zone ON agg_hourly_zone_consumption(zone_id);

-- Monthly Consumption Summary
CREATE TABLE IF NOT EXISTS agg_monthly_consumption (
    agg_id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    meter_id TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    
    total_consumption_kwh REAL,
    avg_daily_consumption_kwh REAL,
    max_daily_consumption_kwh REAL,
    min_daily_consumption_kwh REAL,
    
    total_anomalies INTEGER DEFAULT 0,
    reading_count INTEGER,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (meter_id) REFERENCES dim_meter(meter_id),
    FOREIGN KEY (zone_id) REFERENCES dim_zone(zone_id)
);

CREATE INDEX idx_agg_monthly_period ON agg_monthly_consumption(year, month);
CREATE INDEX idx_agg_monthly_meter ON agg_monthly_consumption(meter_id);
CREATE INDEX idx_agg_monthly_zone ON agg_monthly_consumption(zone_id);

-- ============================================================================
-- DATA QUALITY AND VALIDATION TABLES
-- ============================================================================

-- Data Quality Log
CREATE TABLE IF NOT EXISTS dq_quality_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    check_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    total_rows INTEGER,
    null_count INTEGER,
    duplicate_count INTEGER,
    anomaly_count INTEGER,
    out_of_range_count INTEGER,
    
    null_percentage REAL,
    duplicate_percentage REAL,
    anomaly_percentage REAL,
    out_of_range_percentage REAL,
    
    quality_score REAL,
    status TEXT,
    notes TEXT
);

-- Data Anomaly Records
CREATE TABLE IF NOT EXISTS dq_anomalies (
    anomaly_id INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_id INTEGER,
    timestamp DATETIME,
    meter_id TEXT,
    zone_id TEXT,
    
    anomaly_type TEXT,  -- 'outlier', 'inconsistency', 'missing_spike'
    anomaly_column TEXT,
    expected_value REAL,
    actual_value REAL,
    deviation_percentage REAL,
    
    flagged_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    reviewed BOOLEAN DEFAULT 0,
    resolution TEXT,
    
    FOREIGN KEY (meter_id) REFERENCES dim_meter(meter_id),
    FOREIGN KEY (zone_id) REFERENCES dim_zone(zone_id)
);

CREATE INDEX idx_anomalies_timestamp ON dq_anomalies(timestamp);
CREATE INDEX idx_anomalies_meter ON dq_anomalies(meter_id);
CREATE INDEX idx_anomalies_reviewed ON dq_anomalies(reviewed);

-- ============================================================================
-- METADATA TABLES
-- ============================================================================

-- Pipeline Execution Log
CREATE TABLE IF NOT EXISTS meta_pipeline_execution (
    execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_start DATETIME,
    execution_end DATETIME,
    
    stage TEXT,  -- 'ingestion', 'cleaning', 'feature_engineering', 'validation', 'storage'
    status TEXT,  -- 'success', 'failed', 'warning'
    
    rows_input INTEGER,
    rows_output INTEGER,
    records_modified INTEGER,
    
    duration_seconds REAL,
    notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- ETL Configuration History
CREATE TABLE IF NOT EXISTS meta_configuration (
    config_id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_key TEXT UNIQUE NOT NULL,
    config_value TEXT,
    
    effective_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    notes TEXT
);

-- ============================================================================
-- INITIALIZATION OF DIMENSION DATA
-- ============================================================================

-- Insert default load profiles
INSERT OR IGNORE INTO dim_load_profile (load_category, min_power_kW, max_power_kW, description)
VALUES
    ('Low', 0, 1.0, 'Low consumption - minimal appliances running'),
    ('Medium', 1.0, 3.0, 'Medium consumption - normal household operations'),
    ('High', 3.0, 10.0, 'High consumption - heavy appliances in use'),
    ('VeryHigh', 10.0, 25.0, 'Very high consumption - peak demand period');

-- ============================================================================
-- VIEWS FOR REPORTING AND ANALYTICS
-- ============================================================================

-- Current Meter Status View
CREATE VIEW IF NOT EXISTS v_current_meter_status AS
SELECT 
    m.meter_id,
    m.meter_name,
    z.zone_id,
    z.zone_name,
    m.status,
    MAX(f.timestamp) AS last_reading_timestamp,
    COUNT(f.reading_id) AS total_readings,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count
FROM dim_meter m
LEFT JOIN dim_zone z ON 1=1
LEFT JOIN fact_meter_readings f ON m.meter_id = f.meter_id
GROUP BY m.meter_id, m.meter_name, z.zone_id, z.zone_name, m.status;

-- Daily Consumption Comparison View
CREATE VIEW IF NOT EXISTS v_daily_consumption_comparison AS
SELECT 
    agg_date.reading_date,
    zone_summary.zone_id,
    zone_summary.zone_name,
    SUM(agg_date.total_consumption_kwh) AS zone_total_kwh,
    AVG(agg_date.avg_power_kw) AS zone_avg_power_kw,
    COUNT(DISTINCT agg_date.meter_id) AS meter_count
FROM agg_daily_consumption agg_date
LEFT JOIN dim_zone zone_summary ON agg_date.zone_id = zone_summary.zone_id
GROUP BY agg_date.reading_date, zone_summary.zone_id;

-- Peak Hour Performance View
CREATE VIEW IF NOT EXISTS v_peak_hour_performance AS
SELECT 
    f.hour_of_day,
    f.zone_id,
    COUNT(*) AS reading_count,
    AVG(f.active_power_kw) AS avg_load_kw,
    MAX(f.active_power_kw) AS peak_load_kw,
    MIN(f.active_power_kw) AS min_load_kw,
    AVG(f.power_factor) AS avg_power_factor,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count
FROM fact_meter_readings f
WHERE f.peak_hour_flag = 1
GROUP BY f.hour_of_day, f.zone_id;
