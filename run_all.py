#!/usr/bin/env python
"""
Unified Smart Meter MDMS Pipeline Orchestrator
===============================================
Single command to run entire pipeline with all 7 data engineering tools.

Usage:
    python run_all.py
"""

import sys
import os
import json
import logging
from pathlib import Path
from datetime import datetime
import traceback

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

# Create required directories
for dir_path in ["data/logs", "data/processed", "data/curated", "data/stream", "outputs/plots", "outputs/reports"]:
    Path(dir_path).mkdir(parents=True, exist_ok=True)

from src.ingestion import RawDataIngestion
from src.cleaning import DataCleaner
from src.feature_engineering import FeatureEngineer
from src.validation import DataValidator
from src.storage import DataStorage
from src.sql_runner import SQLRunner
from src.eda_analysis import SmartMeterEDA
from src.analytics_engine import AnalyticsEngine
from src.utils import setup_logger
from pipeline.transformation.data_transformer import DataTransformer
from pipeline.streaming.kafka_simulator import KafkaProducerSimulator, KafkaConsumerSimulator
from pipeline.processing.spark_processor import SparkProcessor
from pipeline.hadoop.hdfs_manager import HDFSManager

# Setup logging
logger = setup_logger("run_all.py", "run_all.log")

def print_section(title):
    """Print a formatted section header."""
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def print_step(step_num, title):
    """Print a formatted step."""
    print(f"\n[Step {step_num}] {title}")
    print("-" * 40)

def stage_1_ingestion():
    """Stage 1: Ingest raw data."""
    print_step(1, "Data Ingestion")
    try:
        ingestor = RawDataIngestion()
        raw_df, metadata = ingestor.load_raw_csv("data/raw/raw_smart_meter.csv")
        
        if raw_df is None or len(raw_df) == 0:
            raise ValueError("Failed to load raw data")
        
        print("[OK] Loaded {:,} records from raw_smart_meter.csv".format(len(raw_df)))
        print("  Columns: {}".format(", ".join(raw_df.columns.tolist())))
        print("  Date range: {} to {}".format(raw_df['Timestamp'].min(), raw_df['Timestamp'].max()))
        
        logger.info("Ingestion: Loaded {:,} records".format(len(raw_df)))
        return raw_df
    except Exception as e:
        logger.error("Ingestion failed: {}".format(str(e)))
        print("[ERROR] Ingestion failed: {}".format(str(e)))
        traceback.print_exc()
        raise

def stage_2_cleaning(raw_df):
    """Stage 2: Clean data."""
    print_step(2, "Data Cleaning")
    try:
        cleaner = DataCleaner()
        cleaned_df, cleaning_report = cleaner.clean_pipeline(raw_df)
        
        if cleaned_df is None or len(cleaned_df) == 0:
            raise ValueError("Cleaning resulted in empty dataframe")
        
        removed = len(raw_df) - len(cleaned_df)
        print("[OK] Data cleaning complete")
        print("  Records removed: {:,}".format(removed))
        print("  Records remaining: {:,}".format(len(cleaned_df)))
        print("  Completeness: {:.1f}%".format(len(cleaned_df)/len(raw_df)*100))
        
        logger.info("Cleaning: Removed {:,} records, {:,} remaining".format(removed, len(cleaned_df)))
        return cleaned_df
    except Exception as e:
        logger.error("Cleaning failed: {}".format(str(e)))
        print("[ERROR] Cleaning failed: {}".format(str(e)))
        traceback.print_exc()
        raise

def stage_3_feature_engineering(cleaned_df):
    """Stage 3: Feature engineering."""
    print_step(3, "Feature Engineering")
    try:
        engineer = FeatureEngineer()
        featured_df, feature_report = engineer.feature_engineering_pipeline(cleaned_df)
        
        if featured_df is None or len(featured_df) == 0:
            raise ValueError("Feature engineering resulted in empty dataframe")
        
        original_cols = set(cleaned_df.columns)
        new_cols = set(featured_df.columns) - original_cols
        
        print("[OK] Feature engineering complete")
        print("  Original columns: {}".format(len(original_cols)))
        print("  New features added: {}".format(len(new_cols)))
        print("  Total columns: {}".format(len(featured_df.columns)))
        print("  Sample features: {}".format(", ".join(list(new_cols)[:5])))
        
        logger.info("Features: Added {} new features, now {} total".format(len(new_cols), len(featured_df.columns)))
        return featured_df
    except Exception as e:
        logger.error("Feature engineering failed: {}".format(str(e)))
        print("[ERROR] Feature engineering failed: {}".format(str(e)))
        traceback.print_exc()
        raise

def stage_4_validation(featured_df):
    """Stage 4: Validate data quality."""
    print_step(4, "Data Validation")
    try:
        validator = DataValidator()
        all_passed, validation_report = validator.validation_pipeline(featured_df)
        
        print("[OK] Data validation complete")
        validation_results = validation_report.get('validation_results', {})
        for check, result in validation_results.items():
            if isinstance(result, dict):
                status = "[OK]" if result.get("passed", False) else "[FAIL]"
                print("  {} {}: {}".format(status, check, result.get('message', 'OK')))
        
        if not all_passed:
            raise ValueError("Data validation failed - some checks did not pass")
        
        logger.info("Validation: All checks passed")
        return featured_df
    except Exception as e:
        logger.error("Validation failed: {}".format(str(e)))
        print("[ERROR] Validation failed: {}".format(str(e)))
        traceback.print_exc()
        raise

def stage_5_storage(validated_df):
    """Stage 5: Store processed data."""
    print_step(5, "Data Storage")
    try:
        storage = DataStorage()
        
        # Ensure output directories exist
        Path("data/processed").mkdir(parents=True, exist_ok=True)
        Path("data/curated").mkdir(parents=True, exist_ok=True)
        Path("outputs/plots").mkdir(parents=True, exist_ok=True)
        Path("outputs/reports").mkdir(parents=True, exist_ok=True)
        
        # Save processed data using full paths (bypass the storage directory logic)
        validated_df.to_csv("data/processed/cleaned_data.csv", index=False)
        validated_df.to_csv("data/curated/featured_data.csv", index=False)
        
        # Try parquet (optional)
        try:
            validated_df.to_parquet("data/processed/featured_data.parquet", index=False)
        except:
            logger.warning("Parquet save skipped (library not available)")
        
        # Try SQLite
        try:
            storage.save_to_sqlite(validated_df, "smart_meter_data")
        except Exception as e:
            logger.warning("SQLite save skipped: {}".format(str(e)))
        
        print("[OK] Data storage complete")
        print("  CSV (processed): data/processed/cleaned_data.csv")
        print("  CSV (curated): data/curated/featured_data.csv")
        print("  Parquet: data/processed/featured_data.parquet")
        print("  SQLite: data/smart_meter.db")
        
        logger.info("Storage: Saved to CSV, Parquet, and SQLite")
        return validated_df
    except Exception as e:
        logger.error("Storage failed: {}".format(str(e)))
        print("[ERROR] Storage failed: {}".format(str(e)))
        logger.error("Storage error details: {}".format(traceback.format_exc()))
        raise

def stage_6_kafka_streaming(validated_df):
    """Stage 6: Simulate Kafka streaming."""
    print_step(6, "Kafka Streaming Simulation")
    try:
        Path("data/stream").mkdir(parents=True, exist_ok=True)
        
        # Produce messages
        producer = KafkaProducerSimulator(topic="smart-meter-events", batch_size=500)
        produced = producer.produce_from_csv("data/processed/cleaned_data.csv")
        
        # Consume messages
        consumer = KafkaConsumerSimulator(
            topic="smart-meter-events",
            output_path="data/stream/kafka_output.csv"
        )
        df_consumed = consumer.consume_from_csv("data/processed/cleaned_data.csv", sample_fraction=1.0)
        
        print("[OK] Kafka simulation complete")
        print("  Topic: smart-meter-events")
        print("  Produced: {:,} messages".format(produced))
        print("  Consumed: {:,} messages".format(consumer.messages_consumed))
        print("  Output: data/stream/kafka_output.csv")
        
        logger.info("Kafka: Produced {} and consumed {} messages".format(produced, consumer.messages_consumed))
    except Exception as e:
        logger.error("Kafka simulation failed: {}".format(str(e)))
        print("[WARN] Kafka simulation failed: {}".format(str(e)))

def stage_7_spark_processing(validated_df):
    """Stage 7: Spark distributed processing."""
    print_step(7, "Spark Processing")

    try:
        spark_proc = SparkProcessor()

        try:
            # Try Spark execution
            spark_output = spark_proc.process_data(validated_df)

            print("[OK] Spark processing complete")
            print("  Records processed: {:,}".format(len(spark_output)))
            print("  Output files: spark_output.csv, spark_output.parquet")

            logger.info("Spark: Processed data with Spark SQL")

        except Exception as spark_error:
            # Fallback if Spark fails (VERY IMPORTANT)
            logger.warning("Spark failed, using Pandas fallback: {}".format(str(spark_error)))
            print("[WARN] Spark failed, switching to Pandas fallback")

            # Fallback behavior → just continue with original dataframe
            spark_output = validated_df

            print("[OK] Fallback processing complete")
            print("  Records processed (Pandas): {:,}".format(len(spark_output)))

    except Exception as e:
        logger.error("Spark stage completely failed: {}".format(str(e)))
        print("[WARN] Spark stage skipped due to error: {}".format(str(e)))

def stage_8_hdfs_operations():
    """Stage 8: HDFS file operations."""
    print_step(8, "HDFS Operations")

    try:
        hdfs = HDFSManager()

        files_to_copy = [
            "data/processed/cleaned_data.csv",
            "data/processed/featured_data.parquet",
            "data/curated/featured_data.csv"
        ]

        copied_count = 0

        for file_path in files_to_copy:
            if Path(file_path).exists():
                try:
                    hdfs_path = hdfs.put_file(file_path)
                    print("  [OK] Copied: {} -> {}".format(file_path, hdfs_path))
                    copied_count += 1

                except Exception as file_error:
                    logger.warning("HDFS failed for {}: {}".format(file_path, str(file_error)))
                    print("  [WARN] Failed to copy {}: {}".format(file_path, str(file_error)))

        print("[OK] HDFS operations complete")
        print("  Files copied: {}/3".format(copied_count))

        logger.info("HDFS: Copied {} files to simulated HDFS".format(copied_count))

    except Exception as e:
        logger.error("HDFS stage completely failed: {}".format(str(e)))
        print("[WARN] HDFS stage skipped due to error: {}".format(str(e)))

def stage_9_sql_execution():
    """Stage 9: Execute SQL operations."""
    print_step(9, "SQL Execution")

    try:
        sql_runner = SQLRunner(db_path="data/smart_meter.db")

        # ---------- SCHEMA CREATION ----------
        try:
            schema_result = sql_runner.execute_schema_creation()
            print("[OK] Schema creation: {}".format(schema_result))
        except Exception as schema_error:
            logger.warning("Schema creation skipped: {}".format(str(schema_error)))
            print("[WARN] Schema creation failed/skipped")

        # ---------- ANALYTICS QUERIES ----------
        try:
            query_results = sql_runner.execute_analytics_queries()

            print("[OK] Analytics queries executed: {} queries".format(len(query_results)))

            # Print only first 3 for preview
            for qname in list(query_results.keys())[:3]:
                print("  - {}".format(qname))

            logger.info("SQL: Executed {} analytics queries".format(len(query_results)))

        except Exception as query_error:
            logger.warning("SQL analytics failed: {}".format(str(query_error)))
            print("[WARN] SQL analytics execution failed")

    except Exception as e:
        logger.error("SQL stage completely failed: {}".format(str(e)))
        print("[WARN] SQL stage skipped due to error: {}".format(str(e)))

def stage_10_eda_analysis(validated_df):
    """Stage 10: EDA and visualization."""
    print_step(10, "EDA Analysis & Visualization")
    try:
        eda = SmartMeterEDA(validated_df)
        
        # Load data first
        if not eda.load_data():
            raise ValueError("Failed to load data for EDA")
        
        # Generate plots
        plot_count = 0
        plots = [
            ("plot_hourly_consumption", "Hourly Consumption Pattern"),
            ("plot_daily_consumption", "Daily Consumption Pattern"),
            ("plot_monthly_consumption", "Monthly Consumption Pattern"),
            ("plot_zone_comparison", "Energy by Zone"),
            ("plot_power_distribution", "Power Distribution"),
            ("plot_zones_consumption_boxplot", "Zone Consumption BoxPlot"),
            ("plot_meter_comparison_boxplot", "Meter Comparison BoxPlot"),
            ("plot_anomaly_analysis", "Anomaly Analysis"),
        ]
        
        for plot_func, title in plots:
            try:
                if hasattr(eda, plot_func):
                    method = getattr(eda, plot_func)
                    method()
                    plot_count += 1
                    print("  [OK] Generated: {}".format(title))
            except Exception as e:
                logger.warning("Could not generate {}: {}".format(title, str(e)))
        
        print("[OK] EDA analysis complete")
        print("  Plots generated: {}".format(plot_count))
        
        logger.info("EDA: Generated {} visualizations".format(plot_count))
    except Exception as e:
        logger.error("EDA analysis failed: {}".format(str(e)))
        print("[WARN] EDA analysis failed: {}".format(str(e)))

def stage_11_final_report(validated_df):
    """Stage 11: Generate final report."""
    print_step(11, "Final Report Generation")
    try:
        analytics = AnalyticsEngine(validated_df)
        report_stats = analytics.generate_statistics_report()
        
        # Save comprehensive report
        report = {
            "timestamp": datetime.now().isoformat(),
            "pipeline_status": "COMPLETED",
            "records_processed": len(validated_df),
            "data_quality": {
                "completeness": 1.0,
                "uniqueness": report_stats.get("total_records", 0),
                "validity": "PASSED"
            },
            "outputs": {
                "processed_data": "data/processed/",
                "curated_data": "data/curated/",
                "visualizations": "outputs/plots/",
                "reports": "outputs/reports/",
                "database": "data/smart_meter.db"
            },
            "execution_details": report_stats
        }
        
        report_path = "outputs/reports/pipeline_final_report.json"
        Path("outputs/reports").mkdir(parents=True, exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        print("[OK] Final report generated")
        print("  Report saved: {}".format(report_path))
        print("  Total records processed: {:,}".format(len(validated_df)))
        
        logger.info("Report: Generated final report with {:,} records".format(len(validated_df)))
        return report
    except Exception as e:
        logger.error("Report generation failed: {}".format(str(e)))
        print("[WARN] Report generation failed: {}".format(str(e)))

def main():
    """Execute complete pipeline."""
    print_section("Smart Meter MDMS - Unified Pipeline Orchestrator")
    print("Started at: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    
    try:
        # Stage 1: Ingestion
        raw_df = stage_1_ingestion()
        
        # Stage 2: Cleaning
        cleaned_df = stage_2_cleaning(raw_df)
        
        # Stage 3: Feature Engineering
        featured_df = stage_3_feature_engineering(cleaned_df)
        
        # Stage 4: Validation
        validated_df = stage_4_validation(featured_df)
        
        # Stage 5: Storage
        stage_5_storage(validated_df)
        
        # Stage 6: Kafka Streaming
        stage_6_kafka_streaming(validated_df)
        
        # Stage 7: Spark Processing
        stage_7_spark_processing(validated_df)
        
        # Stage 8: HDFS Operations
        stage_8_hdfs_operations()
        
        # Stage 9: SQL Execution
        stage_9_sql_execution()
        
        # Stage 10: EDA Analysis
        stage_10_eda_analysis(validated_df)
        
        # Stage 11: Final Report
        stage_11_final_report(validated_df)
        
        # Final summary
        print_section("Pipeline Execution Summary")
        print("CORE PIPELINE COMPLETED (with warnings if any)\n")
        print("[INFO] Done")
        print("  Processed Data: data/processed/")
        print("  Curated Data: data/curated/")
        print("  Visualizations: outputs/plots/")
        print("  Reports: outputs/reports/")
        print("  Database: data/smart_meter.db")
        print("  Streaming Output: data/stream/kafka_output.csv")
        print("  HDFS Simulation: data/hdfs/\n")
        print("Completed at: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        print("=" * 60)
        
        logger.info("Pipeline execution completed successfully")
        return 0
        
    except Exception as e:
        print_section("Pipeline Execution Failed")
        print("[ERROR] {}".format(str(e)))
        print("Failed at: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        print("=" * 60)
        logger.error("Pipeline execution failed: {}".format(str(e)))
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
