-- Smart Meter MDMS Analytics Queries
-- Production-ready SQL queries for business intelligence and reporting

-- ============================================================================
-- 1. CONSUMPTION ANALYTICS
-- ============================================================================

-- Q1.1: Daily Consumption by Meter
CREATE VIEW IF NOT EXISTS analytics_daily_consumption_by_meter AS
SELECT 
    CAST(f.timestamp AS DATE) AS reading_date,
    f.meter_id,
    m.meter_name,
    f.zone_id,
    z.zone_name,
    SUM(f.active_power_kw) AS total_consumption_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    MAX(f.active_power_kw) AS max_power_kw,
    MIN(f.active_power_kw) AS min_power_kw,
    STDDEV(f.active_power_kw) AS stddev_power_kw,
    COUNT(*) AS reading_count
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
LEFT JOIN dim_zone z ON f.zone_id = z.zone_id
GROUP BY CAST(f.timestamp AS DATE), f.meter_id, f.zone_id;

-- Q1.2: Monthly Consumption Trend
CREATE VIEW IF NOT EXISTS analytics_monthly_consumption_trend AS
SELECT 
    f.year,
    f.month,
    f.meter_id,
    m.meter_name,
    f.zone_id,
    z.zone_name,
    SUM(f.active_power_kw) AS monthly_consumption_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    COUNT(DISTINCT CAST(f.timestamp AS DATE)) AS days_in_month,
    SUM(f.active_power_kw) / NULLIF(COUNT(DISTINCT CAST(f.timestamp AS DATE)), 0) AS avg_daily_kwh
FROM fact_meter_readings f
WHERE f.month IS NOT NULL AND f.year IS NOT NULL
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
LEFT JOIN dim_zone z ON f.zone_id = z.zone_id
GROUP BY f.year, f.month, f.meter_id, f.zone_id;

-- Q1.3: Year-over-Year Comparison (if multiple years available)
SELECT 
    year,
    month,
    zone_id,
    zone_name,
    SUM(total_consumption_kwh) AS monthly_kwh
FROM (
    SELECT 
        f.year,
        f.month,
        f.zone_id,
        z.zone_name,
        SUM(f.active_power_kw) AS total_consumption_kwh
    FROM fact_meter_readings f
    LEFT JOIN dim_zone z ON f.zone_id = z.zone_id
    GROUP BY f.year, f.month, f.zone_id
) grouped
GROUP BY year, month, zone_id
ORDER BY year DESC, month DESC;

-- ============================================================================
-- 2. PEAK HOURS AND DEMAND ANALYSIS
-- ============================================================================

-- Q2.1: Peak Hour Analysis
CREATE VIEW IF NOT EXISTS analytics_peak_hour_analysis AS
SELECT 
    f.hour_of_day,
    t.season,
    COUNT(*) AS reading_count,
    AVG(f.active_power_kw) AS avg_load_kw,
    MAX(f.active_power_kw) AS peak_load_kw,
    MIN(f.active_power_kw) AS min_load_kw,
    STDDEV(f.active_power_kw) AS stddev_load_kw,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percentage
FROM fact_meter_readings f
LEFT JOIN dim_time t ON f.time_id = t.time_id
WHERE f.hour_of_day IS NOT NULL
GROUP BY f.hour_of_day, t.season
ORDER BY f.hour_of_day, t.season;

-- Q2.2: Peak vs Off-Peak Consumption
SELECT 
    f.meter_id,
    m.meter_name,
    f.zone_id,
    CASE WHEN f.peak_hour_flag = 1 THEN 'Peak' ELSE 'Off-Peak' END AS period_type,
    COUNT(*) AS reading_count,
    SUM(f.active_power_kw) AS total_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    ROUND(100.0 * SUM(f.active_power_kw) / 
        (SELECT SUM(active_power_kw) FROM fact_meter_readings f2 WHERE f2.meter_id = f.meter_id), 2) AS consumption_percentage
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id, f.peak_hour_flag;

-- Q2.3: Daily Load Profile Pattern
SELECT 
    f.hour_of_day,
    f.day_of_week,
    COUNT(*) AS reading_count,
    AVG(f.active_power_kw) AS avg_power_kw,
    MAX(f.active_power_kw) AS max_power_kw,
    MIN(f.active_power_kw) AS min_power_kw
FROM fact_meter_readings f
WHERE f.hour_of_day IS NOT NULL AND f.day_of_week IS NOT NULL
GROUP BY f.hour_of_day, f.day_of_week
ORDER BY f.day_of_week, f.hour_of_day;

-- ============================================================================
-- 3. ZONE-LEVEL ANALYTICS
-- ============================================================================

-- Q3.1: Zone-wise Total Consumption
CREATE VIEW IF NOT EXISTS analytics_zone_consumption_summary AS
SELECT 
    f.zone_id,
    z.zone_name,
    z.district,
    COUNT(DISTINCT f.meter_id) AS meter_count,
    COUNT(*) AS total_readings,
    SUM(f.active_power_kw) AS total_consumption_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    MAX(f.active_power_kw) AS peak_power_kw,
    ROUND(AVG(f.power_factor), 3) AS avg_power_factor,
    ROUND(AVG(f.frequency_hz), 2) AS avg_frequency_hz
FROM fact_meter_readings f
LEFT JOIN dim_zone z ON f.zone_id = z.zone_id
GROUP BY f.zone_id, z.zone_name, z.district;

-- Q3.2: Zone Hourly Consumption Pattern
SELECT 
    f.zone_id,
    z.zone_name,
    f.hour_of_day,
    COUNT(DISTINCT f.meter_id) AS meter_count,
    COUNT(*) AS reading_count,
    SUM(f.active_power_kw) AS total_consumption_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    MAX(f.active_power_kw) AS peak_power_kw
FROM fact_meter_readings f
LEFT JOIN dim_zone z ON f.zone_id = z.zone_id
WHERE f.hour_of_day IS NOT NULL
GROUP BY f.zone_id, f.hour_of_day
ORDER BY f.zone_id, f.hour_of_day;

-- Q3.3: Zone Comparison Report
SELECT 
    t1.zone_id,
    t1.zone_name,
    t1.meter_count,
    t1.total_consumption_kwh,
    t2.avg_consumption_per_meter,
    CASE 
        WHEN t1.total_consumption_kwh > (SELECT AVG(zone_consumption) FROM (
            SELECT f.zone_id, SUM(f.active_power_kw) AS zone_consumption 
            FROM fact_meter_readings f GROUP BY f.zone_id
        ) zone_stats) THEN 'High'
        ELSE 'Low'
    END AS consumption_level
FROM analytics_zone_consumption_summary t1
LEFT JOIN (
    SELECT zone_id, ROUND(AVG(total_consumption_kwh), 2) AS avg_consumption_per_meter
    FROM analytics_zone_consumption_summary
    GROUP BY zone_id
) t2 ON t1.zone_id = t2.zone_id;

-- ============================================================================
-- 4. ANOMALY DETECTION AND DATA QUALITY
-- ============================================================================

-- Q4.1: Anomaly Count by Meter
CREATE VIEW IF NOT EXISTS analytics_anomalies_by_meter AS
SELECT 
    f.meter_id,
    m.meter_name,
    f.zone_id,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percentage
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id;

-- Q4.2: Anomaly Trends Over Time
SELECT 
    CAST(f.timestamp AS DATE) AS reading_date,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percentage
FROM fact_meter_readings f
GROUP BY CAST(f.timestamp AS DATE)
ORDER BY reading_date DESC;

-- Q4.3: Top Meters with Anomalies
SELECT 
    TOP 10
    f.meter_id,
    m.meter_name,
    f.zone_id,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percentage
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id
ORDER BY anomaly_count DESC;

-- Q4.4: Data Quality Summary
SELECT 
    COUNT(*) AS total_readings,
    SUM(CASE WHEN f.voltage_v IS NULL THEN 1 ELSE 0 END) AS null_voltage_count,
    SUM(CASE WHEN f.current_a IS NULL THEN 1 ELSE 0 END) AS null_current_count,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS overall_anomaly_percentage
FROM fact_meter_readings f;

-- ============================================================================
-- 5. LOAD CATEGORY ANALYSIS
-- ============================================================================

-- Q5.1: Load Category Distribution
CREATE VIEW IF NOT EXISTS analytics_load_category_distribution AS
SELECT 
    f.load_category,
    COUNT(*) AS reading_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_meter_readings), 2) AS percentage_of_total,
    AVG(f.active_power_kw) AS avg_power_kw,
    MAX(f.active_power_kw) AS max_power_kw,
    MIN(f.active_power_kw) AS min_power_kw
FROM fact_meter_readings f
WHERE f.load_category IS NOT NULL
GROUP BY f.load_category
ORDER BY reading_count DESC;

-- Q5.2: Load Category by Meter
SELECT 
    f.meter_id,
    m.meter_name,
    f.load_category,
    COUNT(*) AS reading_count,
    ROUND(100.0 * COUNT(*) / 
        (SELECT COUNT(*) FROM fact_meter_readings f2 WHERE f2.meter_id = f.meter_id), 2) AS percentage
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
WHERE f.load_category IS NOT NULL
GROUP BY f.meter_id, f.load_category
ORDER BY f.meter_id, reading_count DESC;

-- ============================================================================
-- 6. POWER QUALITY METRICS
-- ============================================================================

-- Q6.1: Power Factor Analysis
SELECT 
    CAST(f.timestamp AS DATE) AS reading_date,
    f.zone_id,
    ROUND(AVG(f.power_factor), 3) AS avg_power_factor,
    ROUND(MIN(f.power_factor), 3) AS min_power_factor,
    ROUND(MAX(f.power_factor), 3) AS max_power_factor,
    COUNT(*) AS reading_count,
    SUM(CASE WHEN f.power_factor < 0.95 THEN 1 ELSE 0 END) AS low_power_factor_count
FROM fact_meter_readings f
GROUP BY CAST(f.timestamp AS DATE), f.zone_id
ORDER BY reading_date DESC, f.zone_id;

-- Q6.2: Frequency Stability
SELECT 
    CAST(f.timestamp AS DATE) AS reading_date,
    f.zone_id,
    ROUND(AVG(f.frequency_hz), 3) AS avg_frequency_hz,
    ROUND(STDDEV(f.frequency_hz), 3) AS stddev_frequency,
    ROUND(MIN(f.frequency_hz), 3) AS min_frequency_hz,
    ROUND(MAX(f.frequency_hz), 3) AS max_frequency_hz,
    SUM(CASE WHEN ABS(f.frequency_hz - 50.0) > 0.5 THEN 1 ELSE 0 END) AS out_of_spec_count
FROM fact_meter_readings f
GROUP BY CAST(f.timestamp AS DATE), f.zone_id;

-- Q6.3: Voltage Stability
SELECT 
    f.zone_id,
    ROUND(AVG(f.voltage_v), 2) AS avg_voltage_v,
    ROUND(STDDEV(f.voltage_v), 2) AS stddev_voltage,
    ROUND(MIN(f.voltage_v), 2) AS min_voltage_v,
    ROUND(MAX(f.voltage_v), 2) AS max_voltage_v,
    SUM(CASE WHEN f.voltage_v < 190 OR f.voltage_v > 250 THEN 1 ELSE 0 END) AS out_of_spec_count,
    COUNT(*) AS total_readings
FROM fact_meter_readings f
GROUP BY f.zone_id;

-- ============================================================================
-- 7. TOP CONSUMERS AND RANKINGS
-- ============================================================================

-- Q7.1: Top 10 Meters by Consumption
SELECT 
    TOP 10
    f.meter_id,
    m.meter_name,
    f.zone_id,
    z.zone_name,
    SUM(f.active_power_kw) AS total_consumption_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    COUNT(*) AS reading_count
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
LEFT JOIN dim_zone z ON f.zone_id = z.zone_id
GROUP BY f.meter_id, f.zone_id
ORDER BY total_consumption_kwh DESC;

-- Q7.2: Bottom 10 Meters by Consumption (Low Usage)
SELECT 
    TOP 10
    f.meter_id,
    m.meter_name,
    f.zone_id,
    SUM(f.active_power_kw) AS total_consumption_kwh,
    AVG(f.active_power_kw) AS avg_power_kw,
    COUNT(*) AS reading_count
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id
ORDER BY total_consumption_kwh ASC;

-- Q7.3: Meter Ranking by Zone
SELECT 
    f.zone_id,
    f.meter_id,
    m.meter_name,
    SUM(f.active_power_kw) AS zone_meter_consumption_kwh,
    ROW_NUMBER() OVER (PARTITION BY f.zone_id ORDER BY SUM(f.active_power_kw) DESC) AS rank_in_zone
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.zone_id, f.meter_id
ORDER BY f.zone_id, rank_in_zone;

-- ============================================================================
-- 8. TEMPORAL AND SEASONAL ANALYSIS
-- ============================================================================

-- Q8.1: Seasonal Consumption Patterns
SELECT 
    f.season,
    COUNT(*) AS reading_count,
    SUM(f.active_power_kw) AS seasonal_total_kwh,
    AVG(f.active_power_kw) AS seasonal_avg_power_kw,
    MAX(f.active_power_kw) AS seasonal_peak_power_kw,
    ROUND(AVG(f.outdoor_temp_c), 2) AS avg_outdoor_temp_c
FROM fact_meter_readings f
WHERE f.season IS NOT NULL
GROUP BY f.season
ORDER BY 
    CASE WHEN f.season = 'Winter' THEN 1
         WHEN f.season = 'Summer' THEN 2
         WHEN f.season = 'Monsoon' THEN 3
         ELSE 4 END;

-- Q8.2: Day of Week Analysis
SELECT 
    f.day_of_week,
    CASE f.day_of_week 
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END AS day_name,
    COUNT(*) AS reading_count,
    SUM(f.active_power_kw) AS daily_total_kwh,
    AVG(f.active_power_kw) AS daily_avg_power_kw
FROM fact_meter_readings f
WHERE f.day_of_week IS NOT NULL
GROUP BY f.day_of_week
ORDER BY f.day_of_week;

-- ============================================================================
-- 9. SUB-METER ANALYSIS
-- ============================================================================

-- Q9.1: Sub-Meter Consumption (Kitchen vs HVAC)
SELECT 
    f.meter_id,
    m.meter_name,
    f.zone_id,
    SUM(f.active_power_kw) AS total_consumption_kwh,
    SUM(f.sub_meter_kitchen) AS kitchen_consumption_kwh,
    SUM(f.sub_meter_hvac) AS hvac_consumption_kwh,
    ROUND(100.0 * SUM(f.sub_meter_kitchen) / NULLIF(SUM(f.active_power_kw), 0), 2) AS kitchen_percentage,
    ROUND(100.0 * SUM(f.sub_meter_hvac) / NULLIF(SUM(f.active_power_kw), 0), 2) AS hvac_percentage
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id
ORDER BY total_consumption_kwh DESC;

-- ============================================================================
-- 10. COMPARATIVE AND BENCHMARK ANALYSIS
-- ============================================================================

-- Q10.1: Meter Performance vs Zone Average
SELECT 
    f.meter_id,
    m.meter_name,
    f.zone_id,
    AVG(f.active_power_kw) AS meter_avg_power_kw,
    (SELECT AVG(f2.active_power_kw) FROM fact_meter_readings f2 WHERE f2.zone_id = f.zone_id) AS zone_avg_power_kw,
    ROUND(AVG(f.active_power_kw) - 
        (SELECT AVG(f2.active_power_kw) FROM fact_meter_readings f2 WHERE f2.zone_id = f.zone_id), 4) AS variance_kw,
    CASE 
        WHEN AVG(f.active_power_kw) > (SELECT AVG(f2.active_power_kw) FROM fact_meter_readings f2 WHERE f2.zone_id = f.zone_id) THEN 'Above Average'
        ELSE 'Below Average'
    END AS performance_category
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id
ORDER BY variance_kw DESC;
