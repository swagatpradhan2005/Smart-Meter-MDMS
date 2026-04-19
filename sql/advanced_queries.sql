-- Smart Meter MDMS - Advanced SQL Queries
-- Window functions, aggregations, joins, rankings, time series analysis
-- ===============================================================================

-- ===============================================================================
-- 1. HOURLY CONSUMPTION TREND WITH MOVING AVERAGE
-- ===============================================================================
SELECT 
    DATE_TRUNC('hour', Timestamp) AS hour_bucket,
    AVG(Active_Power_kW) AS hourly_avg_power,
    MAX(Active_Power_kW) AS hourly_max_power,
    MIN(Active_Power_kW) AS hourly_min_power,
    COUNT(*) AS record_count,
    ROUND(
        AVG(AVG(Active_Power_kW)) OVER (
            ORDER BY DATE_TRUNC('hour', Timestamp) 
            ROWS BETWEEN 24 PRECEDING AND CURRENT ROW
        ), 2
    ) AS moving_avg_24h
FROM smart_meter_data
GROUP BY DATE_TRUNC('hour', Timestamp)
ORDER BY hour_bucket DESC;


-- ===============================================================================
-- 2. DAILY CONSUMPTION WITH DAY-OF-WEEK ANALYSIS
-- ===============================================================================
SELECT 
    DATE(Timestamp) AS consumption_date,
    EXTRACT(DOW FROM Timestamp)::INTEGER AS day_of_week,
    CASE 
        WHEN EXTRACT(DOW FROM Timestamp) = 0 THEN 'Sunday'
        WHEN EXTRACT(DOW FROM Timestamp) = 1 THEN 'Monday'
        WHEN EXTRACT(DOW FROM Timestamp) = 2 THEN 'Tuesday'
        WHEN EXTRACT(DOW FROM Timestamp) = 3 THEN 'Wednesday'
        WHEN EXTRACT(DOW FROM Timestamp) = 4 THEN 'Thursday'
        WHEN EXTRACT(DOW FROM Timestamp) = 5 THEN 'Friday'
        WHEN EXTRACT(DOW FROM Timestamp) = 6 THEN 'Saturday'
    END AS day_name,
    SUM(Active_Power_kW) AS daily_total_power,
    AVG(Active_Power_kW) AS daily_avg_power,
    STDDEV_POP(Active_Power_kW) AS daily_stddev,
    MAX(Active_Power_kW) AS peak_power,
    MIN(Active_Power_kW) AS min_power
FROM smart_meter_data
GROUP BY DATE(Timestamp), EXTRACT(DOW FROM Timestamp)
ORDER BY consumption_date DESC;


-- ===============================================================================
-- 3. ZONE-WISE CONSUMPTION RANKING
-- ===============================================================================
SELECT 
    Zone,
    COUNT(*) AS total_records,
    ROUND(AVG(Active_Power_kW), 2) AS avg_power_kw,
    ROUND(MAX(Active_Power_kW), 2) AS peak_power_kw,
    ROUND(MIN(Active_Power_kW), 2) AS min_power_kw,
    ROUND(STDDEV_POP(Active_Power_kW), 2) AS stddev_power,
    RANK() OVER (ORDER BY AVG(Active_Power_kW) DESC) AS consumption_rank,
    ROUND(
        AVG(Active_Power_kW) / (SELECT AVG(Active_Power_kW) FROM smart_meter_data) * 100,
        2
    ) AS percentage_of_avg
FROM smart_meter_data
GROUP BY Zone
ORDER BY consumption_rank;


-- ===============================================================================
-- 4. TOP 20 CONSUMING METERS WITH ZONE INFO
-- ===============================================================================
SELECT 
    smd.Meter_ID,
    smd.Zone,
    COUNT(*) AS record_count,
    ROUND(AVG(smd.Active_Power_kW), 3) AS avg_power_kw,
    ROUND(MAX(smd.Active_Power_kW), 3) AS peak_power_kw,
    ROUND(SUM(smd.Active_Power_kW), 2) AS total_power_kwh,
    RANK() OVER (ORDER BY AVG(smd.Active_Power_kW) DESC) AS meter_rank,
    ROUND(
        AVG(smd.Active_Power_kW) / (SELECT AVG(Active_Power_kW) FROM smart_meter_data) * 100,
        2
    ) AS percentage_of_global_avg,
    CASE 
        WHEN AVG(smd.Active_Power_kW) > (SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Active_Power_kW) FROM smart_meter_data) THEN 'VeryHigh'
        WHEN AVG(smd.Active_Power_kW) > (SELECT PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW) FROM smart_meter_data) THEN 'High'
        WHEN AVG(smd.Active_Power_kW) > (SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY Active_Power_kW) FROM smart_meter_data) THEN 'Medium'
        ELSE 'Low'
    END AS consumption_category
FROM smart_meter_data smd
GROUP BY smd.Meter_ID, smd.Zone
ORDER BY avg_power_kw DESC
LIMIT 20;


-- ===============================================================================
-- 5. PEAK HOURS IDENTIFICATION (HIGH DEMAND PERIODS)
-- ===============================================================================
SELECT 
    EXTRACT(HOUR FROM Timestamp) AS hour_of_day,
    COUNT(*) AS record_count,
    ROUND(AVG(Active_Power_kW), 2) AS avg_power_during_hour,
    ROUND(MAX(Active_Power_kW), 2) AS max_power_during_hour,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Active_Power_kW), 2) AS p95_power,
    RANK() OVER (ORDER BY AVG(Active_Power_kW) DESC) AS peak_rank,
    CASE 
        WHEN RANK() OVER (ORDER BY AVG(Active_Power_kW) DESC) <= 5 THEN 'Peak'
        WHEN RANK() OVER (ORDER BY AVG(Active_Power_kW) DESC) BETWEEN 6 AND 12 THEN 'Mid-Peak'
        ELSE 'Off-Peak'
    END AS period_classification
FROM smart_meter_data
GROUP BY EXTRACT(HOUR FROM Timestamp)
ORDER BY hour_of_day;


-- ===============================================================================
-- 6. METER CONSUMPTION TREND WITH LAG (PREV PERIOD COMPARISON)
-- ===============================================================================
SELECT 
    Meter_ID,
    DATE_TRUNC('day', Timestamp) AS day_bucket,
    ROUND(SUM(Active_Power_kW), 2) AS daily_consumption,
    ROUND(
        LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID 
            ORDER BY DATE_TRUNC('day', Timestamp)
        ), 2
    ) AS prev_day_consumption,
    ROUND(
        ((SUM(Active_Power_kW) - LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID 
            ORDER BY DATE_TRUNC('day', Timestamp)
        )) / LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID 
            ORDER BY DATE_TRUNC('day', Timestamp)
        ) * 100), 2
    ) AS percent_change,
    LEAD(SUM(Active_Power_kW)) OVER (
        PARTITION BY Meter_ID 
        ORDER BY DATE_TRUNC('day', Timestamp)
    ) AS next_day_consumption
FROM smart_meter_data
GROUP BY Meter_ID, DATE_TRUNC('day', Timestamp)
ORDER BY Meter_ID, day_bucket DESC;


-- ===============================================================================
-- 7. ANOMALY DETECTION USING IQR METHOD
-- ===============================================================================
SELECT 
    Meter_ID,
    Timestamp,
    Active_Power_kW,
    Zone,
    Q1,
    Q3,
    IQR,
    LOWER_BOUND,
    UPPER_BOUND,
    CASE 
        WHEN Active_Power_kW < LOWER_BOUND THEN 'Below-Normal'
        WHEN Active_Power_kW > UPPER_BOUND THEN 'Above-Normal'
        ELSE 'Normal'
    END AS status,
    ABS(Active_Power_kW - Q3) AS distance_from_q3
FROM (
    SELECT 
        Meter_ID,
        Timestamp,
        Active_Power_kW,
        Zone,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) AS Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) AS Q3,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) - 
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) AS IQR,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) - 
        1.5 * (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) - 
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID)) AS LOWER_BOUND,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) + 
        1.5 * (PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID) - 
               PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Active_Power_kW) OVER (PARTITION BY Meter_ID)) AS UPPER_BOUND
    FROM smart_meter_data
) anomaly_detection
WHERE Active_Power_kW < LOWER_BOUND OR Active_Power_kW > UPPER_BOUND
ORDER BY Timestamp DESC
LIMIT 100;


-- ===============================================================================
-- 8. POWER FACTOR ANALYSIS WITH RANKINGS
-- ===============================================================================
SELECT 
    Meter_ID,
    Zone,
    ROUND(AVG(Active_Power_kW / NULLIF(Apparent_Power_kVA, 0)), 3) AS avg_power_factor,
    COUNT(CASE WHEN (Active_Power_kW / NULLIF(Apparent_Power_kVA, 0)) < 0.85 THEN 1 END) AS poor_pf_records,
    COUNT(*) AS total_records,
    ROUND(
        100.0 * COUNT(CASE WHEN (Active_Power_kW / NULLIF(Apparent_Power_kVA, 0)) < 0.85 THEN 1 END) / COUNT(*),
        2
    ) AS poor_pf_percentage,
    RANK() OVER (ORDER BY AVG(Active_Power_kW / NULLIF(Apparent_Power_kVA, 0)) ASC) AS low_pf_rank
FROM smart_meter_data
WHERE Apparent_Power_kVA > 0
GROUP BY Meter_ID, Zone
HAVING AVG(Active_Power_kW / NULLIF(Apparent_Power_kVA, 0)) < 0.90
ORDER BY avg_power_factor ASC;


-- ===============================================================================
-- 9. QUARTERLY CONSUMPTION ANALYSIS WITH GROWTH RATE
-- ===============================================================================
SELECT 
    EXTRACT(QUARTER FROM Timestamp) AS quarter,
    EXTRACT(YEAR FROM Timestamp) AS year,
    Zone,
    COUNT(*) AS record_count,
    ROUND(AVG(Active_Power_kW), 2) AS quarterly_avg,
    ROUND(SUM(Active_Power_kW), 2) AS quarterly_total,
    ROUND(
        ((SUM(Active_Power_kW) - LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Zone 
            ORDER BY EXTRACT(YEAR FROM Timestamp), EXTRACT(QUARTER FROM Timestamp)
        )) / LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Zone 
            ORDER BY EXTRACT(YEAR FROM Timestamp), EXTRACT(QUARTER FROM Timestamp)
        ) * 100), 2
    ) AS yoq_growth_rate
FROM smart_meter_data
GROUP BY 
    EXTRACT(QUARTER FROM Timestamp),
    EXTRACT(YEAR FROM Timestamp),
    Zone
ORDER BY year DESC, quarter DESC, Zone;


-- ===============================================================================
-- 10. ROLLING WINDOW AVERAGES (7-day, 30-day)
-- ===============================================================================
SELECT 
    Meter_ID,
    DATE_TRUNC('day', Timestamp) AS day,
    ROUND(AVG(Active_Power_kW), 2) AS daily_avg,
    ROUND(
        AVG(AVG(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID
            ORDER BY DATE_TRUNC('day', Timestamp)
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS avg_7day_rolling,
    ROUND(
        AVG(AVG(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID
            ORDER BY DATE_TRUNC('day', Timestamp)
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 2
    ) AS avg_30day_rolling,
    ROUND(
        STDDEV_SAMP(AVG(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID
            ORDER BY DATE_TRUNC('day', Timestamp)
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2
    ) AS stddev_7day
FROM smart_meter_data
GROUP BY Meter_ID, DATE_TRUNC('day', Timestamp)
ORDER BY Meter_ID, day DESC;


-- ===============================================================================
-- 11. CUMULATIVE CONSUMPTION BY METER
-- ===============================================================================
SELECT 
    Meter_ID,
    Timestamp,
    Active_Power_kW,
    SUM(Active_Power_kW) OVER (
        PARTITION BY Meter_ID 
        ORDER BY Timestamp 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_power,
    ROW_NUMBER() OVER (
        PARTITION BY Meter_ID 
        ORDER BY Timestamp
    ) AS reading_number,
    RANK() OVER (
        PARTITION BY Meter_ID 
        ORDER BY Active_Power_kW DESC
    ) AS power_rank_in_meter
FROM smart_meter_data
ORDER BY Meter_ID, Timestamp DESC
LIMIT 1000;


-- ===============================================================================
-- 12. ZONE COMPARISON WITH STATISTICAL METRICS
-- ===============================================================================
SELECT 
    Zone,
    COUNT(*) AS record_count,
    ROUND(AVG(Active_Power_kW), 2) AS mean,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY Active_Power_kW), 2) AS median,
    ROUND(MODE() WITHIN GROUP (ORDER BY ROUND(Active_Power_kW, 1)), 2) AS mode,
    ROUND(STDDEV_SAMP(Active_Power_kW), 2) AS stddev,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY Active_Power_kW), 2) AS q1,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY Active_Power_kW), 2) AS q3,
    ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY Active_Power_kW), 2) AS p5,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY Active_Power_kW), 2) AS p95,
    MIN(Active_Power_kW) AS min_val,
    MAX(Active_Power_kW) AS max_val,
    ROUND((MAX(Active_Power_kW) - MIN(Active_Power_kW)), 2) AS range
FROM smart_meter_data
GROUP BY Zone
ORDER BY mean DESC;


-- ===============================================================================
-- 13. PEAK DEMAND BY ZONE AND HOUR
-- ===============================================================================
SELECT 
    Zone,
    EXTRACT(HOUR FROM Timestamp) AS hour,
    COUNT(*) AS occurrence_count,
    ROUND(AVG(Active_Power_kW), 2) AS avg_power,
    ROUND(MAX(Active_Power_kW), 2) AS peak_power,
    ROUND(MIN(Active_Power_kW), 2) AS min_power,
    RANK() OVER (
        PARTITION BY Zone 
        ORDER BY AVG(Active_Power_kW) DESC
    ) AS peak_rank_in_zone,
    DENSE_RANK() OVER (
        ORDER BY AVG(Active_Power_kW) DESC
    ) AS global_peak_rank
FROM smart_meter_data
GROUP BY Zone, EXTRACT(HOUR FROM Timestamp)
ORDER BY Zone, peak_rank_in_zone;


-- ===============================================================================
-- 14. LOAD VOLATILITY ANALYSIS
-- ===============================================================================
SELECT 
    Meter_ID,
    Zone,
    DATE_TRUNC('hour', Timestamp) AS hour_bucket,
    COUNT(*) AS reading_count,
    ROUND(AVG(Active_Power_kW), 2) AS hourly_avg,
    ROUND(STDDEV_SAMP(Active_Power_kW), 2) AS hourly_stddev,
    ROUND(STDDEV_SAMP(Active_Power_kW) / NULLIF(AVG(Active_Power_kW), 0), 2) AS coefficient_of_variation,
    MAX(Active_Power_kW) - MIN(Active_Power_kW) AS range
FROM smart_meter_data
GROUP BY Meter_ID, Zone, DATE_TRUNC('hour', Timestamp)
HAVING STDDEV_SAMP(Active_Power_kW) / NULLIF(AVG(Active_Power_kW), 0) > 0.5
ORDER BY coefficient_of_variation DESC;


-- ===============================================================================
-- 15. TOP CONSUMPTION HOURS BY ZONE (Aggregation with GROUP BY)
-- ===============================================================================
WITH hourly_zone_consumption AS (
    SELECT 
        Zone,
        EXTRACT(HOUR FROM Timestamp) AS hour,
        ROUND(SUM(Active_Power_kW), 2) AS total_power_hour,
        COUNT(*) AS meter_count
    FROM smart_meter_data
    GROUP BY Zone, EXTRACT(HOUR FROM Timestamp)
)
SELECT 
    Zone,
    hour,
    total_power_hour,
    meter_count,
    ROW_NUMBER() OVER (PARTITION BY Zone ORDER BY total_power_hour DESC) AS hour_rank_in_zone,
    RANK() OVER (ORDER BY total_power_hour DESC) AS global_hour_rank
FROM hourly_zone_consumption
WHERE ROW_NUMBER() OVER (PARTITION BY Zone ORDER BY total_power_hour DESC) <= 5
ORDER BY Zone, hour_rank_in_zone;


-- ===============================================================================
-- 16. HISTORICAL CONSUMPTION COMPARISON (OVER TIME)
-- ===============================================================================
SELECT 
    Meter_ID,
    DATE_TRUNC('month', Timestamp) AS month,
    ROUND(SUM(Active_Power_kW), 2) AS monthly_consumption,
    ROUND(AVG(Active_Power_kW), 2) AS monthly_avg,
    LAG(ROUND(SUM(Active_Power_kW), 2)) OVER (
        PARTITION BY Meter_ID 
        ORDER BY DATE_TRUNC('month', Timestamp)
    ) AS prev_month_consumption,
    ROUND(
        ((SUM(Active_Power_kW) - LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID 
            ORDER BY DATE_TRUNC('month', Timestamp)
        )) / LAG(SUM(Active_Power_kW)) OVER (
            PARTITION BY Meter_ID 
            ORDER BY DATE_TRUNC('month', Timestamp)
        ) * 100), 2
    ) AS mom_growth_percent
FROM smart_meter_data
GROUP BY Meter_ID, DATE_TRUNC('month', Timestamp)
ORDER BY Meter_ID, month DESC;


-- ===============================================================================
-- 17. EFFICIENCY SCORE BY METER (Normalized)
-- ===============================================================================
SELECT 
    m.Meter_ID,
    m.Zone,
    ROUND(AVG(m.Active_Power_kW), 2) AS avg_consumption,
    ROUND(
        100 * (1 - (AVG(m.Active_Power_kW) / MAX(global_max.max_consumption))),
        2
    ) AS efficiency_score,
    CASE 
        WHEN 100 * (1 - (AVG(m.Active_Power_kW) / MAX(global_max.max_consumption))) >= 80 THEN 'Excellent'
        WHEN 100 * (1 - (AVG(m.Active_Power_kW) / MAX(global_max.max_consumption))) >= 60 THEN 'Good'
        WHEN 100 * (1 - (AVG(m.Active_Power_kW) / MAX(global_max.max_consumption))) >= 40 THEN 'Fair'
        ELSE 'Needs Improvement'
    END AS efficiency_rating
FROM smart_meter_data m
CROSS JOIN (SELECT MAX(Active_Power_kW) as max_consumption FROM smart_meter_data) global_max
GROUP BY m.Meter_ID, m.Zone, global_max.max_consumption
ORDER BY efficiency_score DESC;


-- ===============================================================================
-- 18. CONSUMPTION SPIKES DETECTION
-- ===============================================================================
SELECT 
    Meter_ID,
    Timestamp,
    Active_Power_kW,
    Zone,
    AVG(Active_Power_kW) OVER (
        PARTITION BY Meter_ID 
        ORDER BY Timestamp 
        ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
    ) AS context_avg,
    Active_Power_kW - AVG(Active_Power_kW) OVER (
        PARTITION BY Meter_ID 
        ORDER BY Timestamp 
        ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
    ) AS deviation,
    CASE 
        WHEN Active_Power_kW > (
            AVG(Active_Power_kW) OVER (
                PARTITION BY Meter_ID 
                ORDER BY Timestamp 
                ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
            ) + 2 * STDDEV_SAMP(Active_Power_kW) OVER (
                PARTITION BY Meter_ID 
                ORDER BY Timestamp 
                ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
            )
        ) THEN 'Spike'
        ELSE 'Normal'
    END AS classification
FROM smart_meter_data
ORDER BY Meter_ID, Timestamp DESC
LIMIT 500;
