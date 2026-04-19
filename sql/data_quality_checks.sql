-- Smart Meter MDMS Data Quality Checks
-- Comprehensive validation and quality assessment queries

-- ============================================================================
-- 1. COMPLETENESS CHECKS - NULL AND EMPTY VALUE ANALYSIS
-- ============================================================================

-- Q1.1: Null Value Counts by Column
SELECT 
    'Timestamp' AS column_name,
    SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_count,
    COUNT(*) AS total_count,
    ROUND(100.0 * SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage
FROM fact_meter_readings
UNION ALL
SELECT 'Meter_ID', SUM(CASE WHEN meter_id IS NULL THEN 1 ELSE 0 END), COUNT(*), 
    ROUND(100.0 * SUM(CASE WHEN meter_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings
UNION ALL
SELECT 'Zone_ID', SUM(CASE WHEN zone_id IS NULL THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN zone_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings
UNION ALL
SELECT 'Voltage_V', SUM(CASE WHEN voltage_v IS NULL THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN voltage_v IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings
UNION ALL
SELECT 'Current_A', SUM(CASE WHEN current_a IS NULL THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN current_a IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings
UNION ALL
SELECT 'Active_Power_kW', SUM(CASE WHEN active_power_kw IS NULL THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN active_power_kw IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings
UNION ALL
SELECT 'Reactive_Power_kW', SUM(CASE WHEN reactive_power_kw IS NULL THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN reactive_power_kw IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings
UNION ALL
SELECT 'Power_Factor', SUM(CASE WHEN power_factor IS NULL THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN power_factor IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings;

-- Q1.2: Meters with Missing Data
SELECT 
    f.meter_id,
    m.meter_name,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN f.voltage_v IS NULL THEN 1 ELSE 0 END) AS voltage_nulls,
    SUM(CASE WHEN f.current_a IS NULL THEN 1 ELSE 0 END) AS current_nulls,
    SUM(CASE WHEN f.active_power_kw IS NULL THEN 1 ELSE 0 END) AS power_nulls,
    ROUND(100.0 * SUM(CASE WHEN f.voltage_v IS NULL OR f.current_a IS NULL OR f.active_power_kw IS NULL THEN 1 ELSE 0 END) 
        / COUNT(*), 2) AS overall_null_percentage
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id
HAVING SUM(CASE WHEN f.voltage_v IS NULL OR f.current_a IS NULL OR f.active_power_kw IS NULL THEN 1 ELSE 0 END) > 0
ORDER BY overall_null_percentage DESC;

-- ============================================================================
-- 2. UNIQUENESS CHECKS - DUPLICATE AND KEY VIOLATION ANALYSIS
-- ============================================================================

-- Q2.1: Duplicate Reading Detection
SELECT 
    timestamp,
    meter_id,
    zone_id,
    COUNT(*) AS duplicate_count
FROM fact_meter_readings
GROUP BY timestamp, meter_id, zone_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Q2.2: Duplicate Count Summary
SELECT 
    COUNT(*) AS total_readings,
    COUNT(DISTINCT reading_id) AS unique_reading_ids,
    COUNT(*) - COUNT(DISTINCT reading_id) AS duplicate_records,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT reading_id)) / COUNT(*), 2) AS duplicate_percentage
FROM fact_meter_readings;

-- Q2.3: Meters with Highest Duplication
SELECT 
    TOP 10
    f.meter_id,
    m.meter_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT CAST(f.timestamp AS DATE)) AS unique_dates,
    COUNT(*) - COUNT(DISTINCT (CAST(f.timestamp AS DATE), f.hour_of_day)) AS duplicate_records
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id
ORDER BY duplicate_records DESC;

-- ============================================================================
-- 3. VALIDITY CHECKS - RANGE AND FORMAT VALIDATION
-- ============================================================================

-- Q3.1: Out-of-Range Value Detection
CREATE VIEW IF NOT EXISTS dq_out_of_range_summary AS
SELECT 
    'Voltage' AS measurement,
    SUM(CASE WHEN voltage_v < 190 OR voltage_v > 250 THEN 1 ELSE 0 END) AS out_of_range_count,
    COUNT(*) AS total_count,
    ROUND(100.0 * SUM(CASE WHEN voltage_v < 190 OR voltage_v > 250 THEN 1 ELSE 0 END) / COUNT(*), 2) AS out_of_range_percentage,
    ROUND(MIN(voltage_v), 2) AS actual_min,
    ROUND(MAX(voltage_v), 2) AS actual_max,
    '190-250V' AS expected_range
FROM fact_meter_readings
UNION ALL
SELECT 'Current', SUM(CASE WHEN current_a < 0 OR current_a > 100 THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN current_a < 0 OR current_a > 100 THEN 1 ELSE 0 END) / COUNT(*), 2),
    ROUND(MIN(current_a), 2), ROUND(MAX(current_a), 2), '0-100A' FROM fact_meter_readings
UNION ALL
SELECT 'Active_Power', SUM(CASE WHEN active_power_kw < 0 OR active_power_kw > 25 THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN active_power_kw < 0 OR active_power_kw > 25 THEN 1 ELSE 0 END) / COUNT(*), 2),
    ROUND(MIN(active_power_kw), 2), ROUND(MAX(active_power_kw), 2), '0-25kW' FROM fact_meter_readings
UNION ALL
SELECT 'Frequency', SUM(CASE WHEN frequency_hz < 49.5 OR frequency_hz > 50.5 THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN frequency_hz < 49.5 OR frequency_hz > 50.5 THEN 1 ELSE 0 END) / COUNT(*), 2),
    ROUND(MIN(frequency_hz), 3), ROUND(MAX(frequency_hz), 3), '49.5-50.5Hz' FROM fact_meter_readings
UNION ALL
SELECT 'Temperature', SUM(CASE WHEN outdoor_temp_c < -10 OR outdoor_temp_c > 50 THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN outdoor_temp_c < -10 OR outdoor_temp_c > 50 THEN 1 ELSE 0 END) / COUNT(*), 2),
    ROUND(MIN(outdoor_temp_c), 2), ROUND(MAX(outdoor_temp_c), 2), '-10 to 50°C' FROM fact_meter_readings;

-- Q3.2: Specific Out-of-Range Records
SELECT 
    TOP 100
    reading_id,
    timestamp,
    meter_id,
    zone_id,
    voltage_v,
    current_a,
    active_power_kw,
    frequency_hz,
    CASE 
        WHEN voltage_v < 190 OR voltage_v > 250 THEN 'Voltage out of range'
        WHEN current_a < 0 OR current_a > 100 THEN 'Current out of range'
        WHEN active_power_kw < 0 OR active_power_kw > 25 THEN 'Power out of range'
        WHEN frequency_hz < 49.5 OR frequency_hz > 50.5 THEN 'Frequency out of range'
    END AS violation_type
FROM fact_meter_readings
WHERE (voltage_v < 190 OR voltage_v > 250)
   OR (current_a < 0 OR current_a > 100)
   OR (active_power_kw < 0 OR active_power_kw > 25)
   OR (frequency_hz < 49.5 OR frequency_hz > 50.5)
ORDER BY timestamp DESC;

-- Q3.3: Zero Voltage with Active Current (Logical Error)
SELECT 
    COUNT(*) AS zero_voltage_nonzero_current_records,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_meter_readings), 2) AS percentage_of_total
FROM fact_meter_readings
WHERE voltage_v = 0 AND current_a > 0;

-- ============================================================================
-- 4. CONSISTENCY CHECKS - BUSINESS RULE VALIDATION
-- ============================================================================

-- Q4.1: Power Calculation Consistency (Apparent = sqrt(Active^2 + Reactive^2))
SELECT 
    TOP 100
    reading_id,
    timestamp,
    meter_id,
    active_power_kw,
    reactive_power_kw,
    apparent_power_kva,
    ROUND(SQRT(active_power_kw * active_power_kw + reactive_power_kw * reactive_power_kw), 3) AS calculated_apparent_power,
    ROUND(ABS(apparent_power_kva - SQRT(active_power_kw * active_power_kw + reactive_power_kw * reactive_power_kw)), 3) AS variance
FROM fact_meter_readings
WHERE ABS(apparent_power_kva - SQRT(active_power_kw * active_power_kw + reactive_power_kw * reactive_power_kw)) > 0.1
ORDER BY variance DESC;

-- Q4.2: Power Factor Consistency
SELECT 
    COUNT(*) AS power_factor_invalid_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM fact_meter_readings), 2) AS percentage_of_total
FROM fact_meter_readings
WHERE power_factor < 0 OR power_factor > 1;

-- Q4.3: Timestamp Continuity Check (5-minute readings expected)
SELECT 
    f1.meter_id,
    m.meter_name,
    f1.timestamp AS expected_time,
    f2.timestamp AS actual_time,
    DATEDIFF(MINUTE, f1.timestamp, f2.timestamp) AS time_gap_minutes,
    CASE 
        WHEN DATEDIFF(MINUTE, f1.timestamp, f2.timestamp) = 15 THEN 'Normal'
        WHEN DATEDIFF(MINUTE, f1.timestamp, f2.timestamp) > 15 THEN 'Missing readings'
        WHEN DATEDIFF(MINUTE, f1.timestamp, f2.timestamp) < 15 THEN 'Duplicate period'
    END AS gap_type
FROM fact_meter_readings f1
LEFT JOIN dim_meter m ON f1.meter_id = m.meter_id
INNER JOIN (
    SELECT meter_id, timestamp, ROW_NUMBER() OVER (PARTITION BY meter_id ORDER BY timestamp) AS rn
    FROM fact_meter_readings
) rn2 ON f1.meter_id = rn2.meter_id AND rn2.rn > 1
LEFT JOIN fact_meter_readings f2 ON f1.meter_id = f2.meter_id 
    AND f1.timestamp < f2.timestamp
    AND ROW_NUMBER() OVER (PARTITION BY f1.meter_id ORDER BY f2.timestamp) = 1
WHERE DATEDIFF(MINUTE, f1.timestamp, f2.timestamp) != 15;

-- ============================================================================
-- 5. ANOMALY CHECKS - STATISTICAL OUTLIER DETECTION
-- ============================================================================

-- Q5.1: Anomaly Summary by Meter
CREATE VIEW IF NOT EXISTS dq_anomaly_summary_by_meter AS
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

-- Q5.2: Anomaly Count by Date
SELECT 
    CAST(timestamp AS DATE) AS reading_date,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percentage
FROM fact_meter_readings
GROUP BY CAST(timestamp AS DATE)
ORDER BY reading_date DESC;

-- Q5.3: High Anomaly Meters
SELECT 
    TOP 20
    f.meter_id,
    m.meter_name,
    f.zone_id,
    SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_count,
    ROUND(100.0 * SUM(CASE WHEN f.is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_percentage,
    COUNT(*) AS total_readings
FROM fact_meter_readings f
LEFT JOIN dim_meter m ON f.meter_id = m.meter_id
GROUP BY f.meter_id, f.zone_id
ORDER BY anomaly_count DESC;

-- ============================================================================
-- 6. OVERALL DATA QUALITY SCORE
-- ============================================================================

-- Q6.1: Comprehensive Data Quality Report
SELECT 
    (SELECT COUNT(*) FROM fact_meter_readings) AS total_records,
    (SELECT COUNT(*) FROM dim_meter) AS total_meters,
    (SELECT COUNT(*) FROM dim_zone) AS total_zones,
    (SELECT COUNT(DISTINCT CAST(timestamp AS DATE)) FROM fact_meter_readings) AS days_covered,
    (SELECT ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT reading_id)) / COUNT(*), 2) FROM fact_meter_readings) AS duplicate_percentage,
    (SELECT ROUND(100.0 * SUM(CASE WHEN voltage_v IS NULL OR current_a IS NULL OR active_power_kw IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings) AS null_percentage,
    (SELECT ROUND(100.0 * SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) FROM fact_meter_readings) AS anomaly_percentage,
    ROUND(100.0 * (
        (SELECT COUNT(*) FROM fact_meter_readings) 
        - (SELECT COUNT(*) FROM fact_meter_readings WHERE is_anomaly = 1 OR voltage_v IS NULL OR current_a IS NULL)
    ) / (SELECT COUNT(*) FROM fact_meter_readings), 2) AS overall_quality_score_percentage;

-- Q6.2: Quality Trend Analysis
SELECT 
    CAST(timestamp AS DATE) AS reading_date,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN voltage_v IS NULL OR current_a IS NULL OR active_power_kw IS NULL THEN 1 ELSE 0 END) AS null_records,
    SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END) AS anomaly_records,
    SUM(CASE WHEN voltage_v < 190 OR voltage_v > 250 OR current_a < 0 OR current_a > 100 OR active_power_kw < 0 OR active_power_kw > 25 THEN 1 ELSE 0 END) AS out_of_range_records,
    ROUND(100.0 * (
        COUNT(*) - SUM(CASE WHEN voltage_v IS NULL OR current_a IS NULL OR active_power_kw IS NULL THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_anomaly = 1 THEN 1 ELSE 0 END)
    ) / COUNT(*), 2) AS daily_quality_score
FROM fact_meter_readings
GROUP BY CAST(timestamp AS DATE)
ORDER BY reading_date DESC;
