#!/usr/bin/env python
"""
Comprehensive SQL Execution & Output Generation
Executes all SQL queries and generates CSV outputs in data/analytics/
Creates summary JSON in outputs/reports/
"""

import sqlite3
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import logging
import sys

# Setup logging
import logging
logger = logging.getLogger('sql_execution')

if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

class SQLOutputGenerator:
    def __init__(self, db_path="data/smart_meter.db"):
        self.db_path = Path(db_path)
        self.analytics_dir = Path('data/analytics')
        self.reports_dir = Path('outputs/reports')
        self.analytics_dir.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        self.connection = None
        self.generated_files = []
        self.query_summaries = []
        
        logger.info("SQL Output Generator initialized")
    
    def connect(self):
        """Connect to database and load data."""
        try:
            self.connection = sqlite3.connect(str(self.db_path))
            logger.info(f"Connected to {self.db_path}")
            
            # Load featured data into SQLite
            featured_file = Path('data/curated/featured_data.csv')
            if featured_file.exists():
                logger.info(f"Loading {featured_file} into SQLite...")
                df = pd.read_csv(featured_file)
                df.to_sql('smart_meter_data', self.connection, if_exists='replace', index=False)
                logger.info(f"Loaded {len(df)} records into smart_meter_data table")
            else:
                logger.warning(f"{featured_file} not found")
        
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise
    
    def execute_query(self, query_name, query, output_file):
        """Execute a single query and save to CSV."""
        try:
            logger.info(f"Executing: {query_name}")
            df = pd.read_sql_query(query, self.connection)
            
            if len(df) > 0:
                output_path = self.analytics_dir / output_file
                df.to_csv(output_path, index=False)
                logger.info(f"  -> Saved {len(df)} rows to {output_file}")
                
                self.generated_files.append({
                    'filename': output_file,
                    'rows': len(df),
                    'columns': len(df.columns)
                })
                
                self.query_summaries.append({
                    'query_name': query_name,
                    'output_file': output_file,
                    'record_count': int(len(df)),
                    'columns': list(df.columns),
                    'status': 'SUCCESS'
                })
                return True
            else:
                logger.warning(f"  -> Query returned 0 rows")
                self.query_summaries.append({
                    'query_name': query_name,
                    'output_file': output_file,
                    'record_count': 0,
                    'status': 'EMPTY'
                })
                return False
        
        except Exception as e:
            logger.error(f"  Query failed: {e}")
            self.query_summaries.append({
                'query_name': query_name,
                'output_file': output_file,
                'status': 'ERROR',
                'error': str(e)
            })
            return False
    
    def generate_outputs(self):
        """Generate all SQL outputs."""
        logger.info("\n" + "="*80)
        logger.info("GENERATING SQL OUTPUTS")
        logger.info("="*80)
        
        # Query 1: Hourly Consumption
        query1 = """
        SELECT 
            strftime('%Y-%m-%d %H:00:00', Timestamp) AS hour_bucket,
            ROUND(AVG(Active_Power_kW), 2) AS hourly_avg_power,
            MAX(Active_Power_kW) AS hourly_max_power,
            MIN(Active_Power_kW) AS hourly_min_power,
            COUNT(*) AS record_count
        FROM smart_meter_data
        GROUP BY strftime('%Y-%m-%d %H:00:00', Timestamp)
        ORDER BY hour_bucket DESC
        """
        self.execute_query("Hourly Consumption", query1, "hourly_consumption.csv")
        
        # Query 2: Daily Consumption
        query2 = """
        SELECT 
            DATE(Timestamp) AS consumption_date,
            CASE 
                WHEN strftime('%w', Timestamp) = '0' THEN 'Sunday'
                WHEN strftime('%w', Timestamp) = '1' THEN 'Monday'
                WHEN strftime('%w', Timestamp) = '2' THEN 'Tuesday'
                WHEN strftime('%w', Timestamp) = '3' THEN 'Wednesday'
                WHEN strftime('%w', Timestamp) = '4' THEN 'Thursday'
                WHEN strftime('%w', Timestamp) = '5' THEN 'Friday'
                WHEN strftime('%w', Timestamp) = '6' THEN 'Saturday'
            END AS day_name,
            ROUND(SUM(Active_Power_kW), 2) AS daily_total_power,
            ROUND(AVG(Active_Power_kW), 2) AS daily_avg_power,
            MAX(Active_Power_kW) AS peak_power,
            MIN(Active_Power_kW) AS min_power
        FROM smart_meter_data
        GROUP BY DATE(Timestamp)
        ORDER BY consumption_date DESC
        """
        self.execute_query("Daily Consumption", query2, "daily_consumption.csv")
        
        # Query 3: Zone Analysis (reuse/overwrite)
        query3 = """
        SELECT 
            Zone_ID,
            ROUND(AVG(Active_Power_kW), 2) AS Avg_Power_kW,
            ROUND(MAX(Active_Power_kW), 2) AS Max_Power_kW,
            ROUND(MIN(Active_Power_kW), 2) AS Min_Power_kW,
            COUNT(*) AS Record_Count
        FROM smart_meter_data
        WHERE Zone_ID IS NOT NULL
        GROUP BY Zone_ID
        ORDER BY Avg_Power_kW DESC
        """
        self.execute_query("Zone Analysis", query3, "zone_analysis.csv")
        
        # Query 4: Top Consumers (reuse/overwrite)
        query4 = """
        SELECT 
            Meter_ID,
            Zone_ID,
            COUNT(*) AS record_count,
            ROUND(AVG(Active_Power_kW), 3) AS avg_power_kw,
            ROUND(MAX(Active_Power_kW), 3) AS peak_power_kw,
            ROUND(SUM(Active_Power_kW), 2) AS total_power_kwh
        FROM smart_meter_data
        GROUP BY Meter_ID, Zone_ID
        ORDER BY avg_power_kw DESC
        LIMIT 20
        """
        self.execute_query("Top Consumers", query4, "top_consumers.csv")
        
        # Query 5: Peak Hours
        query5 = """
        SELECT 
            CAST(strftime('%H', Timestamp) AS INTEGER) AS hour_of_day,
            COUNT(*) AS record_count,
            ROUND(AVG(Active_Power_kW), 2) AS avg_power_during_hour,
            ROUND(MAX(Active_Power_kW), 2) AS max_power_during_hour,
            CASE 
                WHEN CAST(strftime('%H', Timestamp) AS INTEGER) BETWEEN 10 AND 21 THEN 'Peak'
                ELSE 'Off-Peak'
            END AS period_classification
        FROM smart_meter_data
        GROUP BY CAST(strftime('%H', Timestamp) AS INTEGER)
        ORDER BY hour_of_day
        """
        self.execute_query("Peak Hours Analysis", query5, "peak_hours.csv")
        
        # Query 6: Anomaly Detection
        query6 = """
        SELECT 
            Meter_ID,
            Zone_ID,
            COUNT(*) AS total_readings,
            ROUND(AVG(Active_Power_kW), 2) AS avg_power_kw,
            ROUND(SQRT(AVG((Active_Power_kW - (SELECT AVG(Active_Power_kW) FROM smart_meter_data)) * 
                           (Active_Power_kW - (SELECT AVG(Active_Power_kW) FROM smart_meter_data)))), 2) AS stddev_power_kw,
            COUNT(CASE WHEN Active_Power_kW > (SELECT AVG(Active_Power_kW) + 2 * 1.5 FROM smart_meter_data) THEN 1 END) AS high_anomalies,
            COUNT(CASE WHEN Active_Power_kW < (SELECT AVG(Active_Power_kW) - 2 * 1.5 FROM smart_meter_data) THEN 1 END) AS low_anomalies
        FROM smart_meter_data
        WHERE Meter_ID IS NOT NULL
        GROUP BY Meter_ID, Zone_ID
        """
        self.execute_query("Anomaly Detection", query6, "anomaly_detection.csv")
        
        # Query 7: Power Factor Analysis
        query7 = """
        SELECT 
            Meter_ID,
            ROUND(AVG(Power_Factor), 3) AS avg_power_factor,
            ROUND(MAX(Power_Factor), 3) AS max_power_factor,
            ROUND(MIN(Power_Factor), 3) AS min_power_factor,
            COUNT(*) AS reading_count
        FROM smart_meter_data
        WHERE Power_Factor IS NOT NULL
        GROUP BY Meter_ID
        ORDER BY avg_power_factor
        """
        self.execute_query("Power Factor Analysis", query7, "power_factor_analysis.csv")
        
        # Query 8: Quarterly Growth
        query8 = """
        SELECT 
            strftime('%Y', Timestamp) AS year,
            CASE 
                WHEN strftime('%m', Timestamp) IN ('01', '02', '03') THEN 'Q1'
                WHEN strftime('%m', Timestamp) IN ('04', '05', '06') THEN 'Q2'
                WHEN strftime('%m', Timestamp) IN ('07', '08', '09') THEN 'Q3'
                ELSE 'Q4'
            END AS quarter,
            ROUND(SUM(Active_Power_kW), 2) AS quarterly_consumption,
            ROUND(AVG(Active_Power_kW), 2) AS avg_power_kw,
            COUNT(*) AS reading_count
        FROM smart_meter_data
        GROUP BY year, quarter
        ORDER BY year, quarter
        """
        self.execute_query("Quarterly Growth", query8, "quarterly_growth.csv")
        
        # Query 9: Moving Average (24h)
        query9 = """
        SELECT 
            strftime('%Y-%m-%d', Timestamp) AS date,
            ROUND(AVG(Active_Power_kW), 2) AS daily_avg,
            COUNT(*) AS readings_per_day
        FROM smart_meter_data
        GROUP BY strftime('%Y-%m-%d', Timestamp)
        ORDER BY date DESC
        """
        self.execute_query("Moving Average 24h", query9, "moving_avg_24h.csv")
        
        # Query 10: Rolling Average
        query10 = """
        SELECT 
            DATE(Timestamp) AS reading_date,
            Meter_ID,
            ROUND(AVG(Active_Power_kW), 2) AS hourly_avg,
            MAX(Active_Power_kW) AS hourly_max,
            MIN(Active_Power_kW) AS hourly_min
        FROM smart_meter_data
        GROUP BY DATE(Timestamp), Meter_ID
        ORDER BY reading_date DESC, Meter_ID
        LIMIT 1000
        """
        self.execute_query("Rolling Average", query10, "rolling_avg.csv")
        
        # Query 11: Cumulative Consumption
        query11 = """
        SELECT 
            DATE(Timestamp) AS reading_date,
            ROUND(SUM(Active_Power_kW), 2) AS daily_consumption,
            ROUND(SUM(SUM(Active_Power_kW)) OVER (ORDER BY DATE(Timestamp)), 2) AS cumulative_consumption
        FROM smart_meter_data
        GROUP BY DATE(Timestamp)
        ORDER BY reading_date
        """
        self.execute_query("Cumulative Consumption", query11, "cumulative_consumption.csv")
        
        # Query 12: Zone Statistics
        query12 = """
        SELECT 
            Zone_ID,
            COUNT(DISTINCT Meter_ID) AS meter_count,
            COUNT(*) AS total_readings,
            ROUND(AVG(Active_Power_kW), 2) AS avg_power_kw,
            ROUND(SQRT(AVG((Active_Power_kW - (SELECT AVG(Active_Power_kW) FROM smart_meter_data)) * 
                           (Active_Power_kW - (SELECT AVG(Active_Power_kW) FROM smart_meter_data)))), 2) AS stddev_power_kw,
            ROUND(MAX(Active_Power_kW), 2) AS peak_power_kw,
            ROUND(MIN(Active_Power_kW), 2) AS min_power_kw
        FROM smart_meter_data
        WHERE Zone_ID IS NOT NULL
        GROUP BY Zone_ID
        ORDER BY avg_power_kw DESC
        """
        self.execute_query("Zone Statistics", query12, "zone_statistics.csv")
        
        # Query 13: Efficiency Scores
        query13 = """
        SELECT 
            Meter_ID,
            Zone_ID,
            COUNT(*) AS total_readings,
            ROUND(AVG(Active_Power_kW), 2) AS avg_power,
            ROUND(AVG(Reactive_Power_kW), 2) AS avg_reactive,
            ROUND(AVG(Power_Factor), 3) AS avg_power_factor,
            CASE 
                WHEN AVG(Power_Factor) >= 0.95 THEN 'Excellent'
                WHEN AVG(Power_Factor) >= 0.85 THEN 'Good'
                WHEN AVG(Power_Factor) >= 0.75 THEN 'Average'
                ELSE 'Poor'
            END AS efficiency_score
        FROM smart_meter_data
        WHERE Power_Factor IS NOT NULL
        GROUP BY Meter_ID, Zone_ID
        """
        self.execute_query("Efficiency Scores", query13, "efficiency_scores.csv")
        
        # Query 14: Consumption Spikes
        query14 = """
        SELECT 
            Meter_ID,
            Zone_ID,
            DATE(Timestamp) AS spike_date,
            MAX(Active_Power_kW) AS peak_power,
            ROUND(AVG(Active_Power_kW), 2) AS avg_power_that_day,
            ROUND((MAX(Active_Power_kW) - AVG(Active_Power_kW)), 2) AS spike_magnitude
        FROM smart_meter_data
        WHERE Meter_ID IS NOT NULL
        GROUP BY Meter_ID, DATE(Timestamp)
        HAVING MAX(Active_Power_kW) > (SELECT AVG(Active_Power_kW) * 1.5 FROM smart_meter_data)
        ORDER BY spike_magnitude DESC
        LIMIT 500
        """
        self.execute_query("Consumption Spikes", query14, "consumption_spikes.csv")
        
        # Query 15: Load Volatility
        query15 = """
        SELECT 
            DATE(Timestamp) AS reading_date,
            ROUND(SQRT(AVG((Active_Power_kW - (SELECT AVG(Active_Power_kW) FROM smart_meter_data)) * 
                           (Active_Power_kW - (SELECT AVG(Active_Power_kW) FROM smart_meter_data)))), 2) AS daily_volatility,
            ROUND(AVG(Active_Power_kW), 2) AS daily_avg,
            ROUND(MAX(Active_Power_kW), 2) AS daily_peak,
            COUNT(*) AS readings
        FROM smart_meter_data
        GROUP BY DATE(Timestamp)
        ORDER BY daily_volatility DESC
        """
        self.execute_query("Load Volatility", query15, "load_volatility.csv")
    
    def create_summary(self):
        """Create summary JSON file."""
        logger.info("\nCreating SQL Summary JSON...")
        
        summary = {
            "generation_timestamp": datetime.now().isoformat(),
            "total_queries_executed": len(self.query_summaries),
            "successful_queries": sum(1 for q in self.query_summaries if q['status'] == 'SUCCESS'),
            "failed_queries": sum(1 for q in self.query_summaries if q['status'] == 'ERROR'),
            "total_outputs_generated": len(self.generated_files),
            "analytics_directory": str(self.analytics_dir),
            "reports_directory": str(self.reports_dir),
            "generated_csv_files": self.generated_files,
            "query_execution_summary": self.query_summaries
        }
        
        summary_path = self.reports_dir / 'sql_summary.json'
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary created: {summary_path}")
        logger.info(f"  Total Files Generated: {summary['total_outputs_generated']}")
        logger.info(f"  Successful Queries: {summary['successful_queries']}/{summary['total_queries_executed']}")
        
        return summary
    
    def validate_outputs(self):
        """Validate all generated outputs."""
        logger.info("\n" + "="*80)
        logger.info("VALIDATING OUTPUTS")
        logger.info("="*80)
        
        csv_files = list(self.analytics_dir.glob('*.csv'))
        logger.info(f"CSV files in data/analytics/: {len(csv_files)}")
        
        for csv_file in sorted(csv_files):
            size = csv_file.stat().st_size / 1024
            df = pd.read_csv(csv_file)
            logger.info(f"  [OK] {csv_file.name}: {len(df)} rows, {size:.1f} KB")
        
        json_files = list(self.reports_dir.glob('*.json'))
        logger.info(f"\nJSON files in outputs/reports/: {len(json_files)}")
        for json_file in sorted(json_files):
            logger.info(f"  [OK] {json_file.name}")
        
        # Check for non-empty files
        empty_files = [f for f in csv_files if f.stat().st_size == 0]
        if empty_files:
            logger.warning(f"Empty files found: {empty_files}")
        else:
            logger.info("\n[OK] All files non-empty")
        
        if len(csv_files) >= 12:
            logger.info(f"\n[OK] Minimum CSV requirement met: {len(csv_files)}/12")
        else:
            logger.warning(f"[WARN] Only {len(csv_files)} CSV files (need 12+)")
    
    def run(self):
        """Execute full process."""
        try:
            self.connect()
            self.generate_outputs()
            self.create_summary()
            self.validate_outputs()
            logger.info("\n" + "="*80)
            logger.info("[SUCCESS] SQL EXECUTION COMPLETE")
            logger.info("="*80)
        except Exception as e:
            logger.error(f"Process failed: {e}")
            raise
        finally:
            if self.connection:
                self.connection.close()


if __name__ == '__main__':
    generator = SQLOutputGenerator()
    generator.run()
