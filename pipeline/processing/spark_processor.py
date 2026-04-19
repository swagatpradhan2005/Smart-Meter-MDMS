"""
PySpark Processing Module
Provides Spark-based data processing with fallback to Pandas.
Includes DataFrame transformations, Spark SQL, partitioning, and local mode support.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Tuple
import logging
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, when, row_number, dense_rank
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning("PySpark not available - will use Pandas fallback")


class SparkProcessor:
    """Handles Spark and Pandas-based data processing."""
    
    def __init__(self, use_spark: bool = True):
        """
        Initialize processor.
        
        Args:
            use_spark: Attempt to use Spark if available
        """
        self.use_spark = use_spark and SPARK_AVAILABLE
        self.spark_session = None
        
        if self.use_spark:
            try:
                self.spark_session = SparkSession.builder \
                    .appName("SmartMeterMDMS") \
                    .master("local[*]") \
                    .config("spark.sql.shuffle.partitions", "4") \
                    .getOrCreate()
                logger.info("Spark session created successfully")
            except Exception as e:
                logger.warning(f"Failed to create Spark session: {e}, using Pandas fallback")
                self.use_spark = False
        else:
            logger.info("Using Pandas processing (Spark disabled or unavailable)")
    
    def process_with_spark(self, df: pd.DataFrame, output_dir: str) -> Dict[str, str]:
        """
        Process data using PySpark.
        
        Args:
            df: Input Pandas DataFrame
            output_dir: Output directory
            
        Returns:
            Dictionary of output paths
        """
        if not self.spark_session:
            logger.warning("Spark session not available, using Pandas fallback")
            return self.process_with_pandas(df, output_dir)
        
        try:
            logger.info("Processing with PySpark...")
            output_paths = {}
            
            # Convert Pandas to Spark DataFrame
            sdf = self.spark_session.createDataFrame(df)
            
            # Example transformations
            # 1. Add rank column partitioned by Meter_ID
            window_spec = Window.partitionBy("Meter_ID").orderBy("Timestamp")
            sdf = sdf.withColumn("row_number", row_number().over(window_spec))
            
            # 2. Cache for performance
            sdf.cache()
            
            # 3. Write main output
            output_dir_path = Path(output_dir)
            output_dir_path.mkdir(parents=True, exist_ok=True)
            
            spark_output = output_dir_path / "spark_processed"
            sdf.coalesce(1).write.mode("overwrite").csv(str(spark_output), header=True)
            output_paths['spark_csv'] = str(spark_output)
            logger.info(f"Spark CSV output: {spark_output}")
            
            # 4. Write Parquet (Spark native format)
            parquet_output = output_dir_path / "spark_processed.parquet"
            sdf.coalesce(1).write.mode("overwrite").parquet(str(parquet_output))
            output_paths['spark_parquet'] = str(parquet_output)
            logger.info(f"Spark Parquet output: {parquet_output}")
            
            # 5. Spark SQL aggregations
            sdf.createOrReplaceTempView("meter_readings")
            
            # Meter-level summary
            meter_summary = self.spark_session.sql("""
                SELECT 
                    Meter_ID,
                    COUNT(*) as record_count,
                    ROUND(AVG(Active_Power_kW), 3) as avg_power_kW,
                    MAX(Active_Power_kW) as max_power_kW,
                    MIN(Active_Power_kW) as min_power_kW
                FROM meter_readings
                GROUP BY Meter_ID
                ORDER BY avg_power_kW DESC
            """)
            
            summary_output = output_dir_path / "spark_meter_summary.csv"
            meter_summary.coalesce(1).write.mode("overwrite").csv(str(summary_output), header=True)
            output_paths['meter_summary'] = str(summary_output)
            logger.info(f"Spark meter summary: {summary_output}")
            
            # Zone-level summary
            zone_summary = self.spark_session.sql("""
                SELECT 
                    Zone_ID,
                    COUNT(*) as record_count,
                    ROUND(AVG(Active_Power_kW), 3) as avg_power_kW,
                    MAX(Voltage_V) as max_voltage_V
                FROM meter_readings
                GROUP BY Zone_ID
                ORDER BY avg_power_kW DESC
            """)
            
            zone_output = output_dir_path / "spark_zone_summary.csv"
            zone_summary.coalesce(1).write.mode("overwrite").csv(str(zone_output), header=True)
            output_paths['zone_summary'] = str(zone_output)
            logger.info(f"Spark zone summary: {zone_output}")
            
            sdf.unpersist()
            logger.info("PySpark processing completed")
            return output_paths
        
        except Exception as e:
            logger.error(f"Spark processing failed: {e}, falling back to Pandas")
            return self.process_with_pandas(df, output_dir)
    
    def process_with_pandas(self, df: pd.DataFrame, output_dir: str) -> Dict[str, str]:
        """
        Process data using Pandas.
        
        Args:
            df: Input DataFrame
            output_dir: Output directory
            
        Returns:
            Dictionary of output paths
        """
        logger.info("Processing with Pandas...")
        output_paths = {}
        output_dir_path = Path(output_dir)
        output_dir_path.mkdir(parents=True, exist_ok=True)
        
        # 1. Main processed output
        processed_output = output_dir_path / "pandas_processed.csv"
        df.to_csv(processed_output, index=False)
        output_paths['pandas_csv'] = str(processed_output)
        logger.info(f"Pandas CSV output: {processed_output}")
        
        # 2. Parquet output
        try:
            parquet_output = output_dir_path / "pandas_processed.parquet"
            df.to_parquet(parquet_output, index=False)
            output_paths['pandas_parquet'] = str(parquet_output)
            logger.info(f"Pandas Parquet output: {parquet_output}")
        except:
            logger.warning("Parquet not available, skipping")
        
        # 3. Meter-level aggregations
        meter_agg = df.groupby('Meter_ID')['Active_Power_kW'].agg(['count', 'mean', 'max', 'min'])
        meter_summary = meter_agg.reset_index().rename(columns={
            'count': 'record_count',
            'mean': 'avg_power_kW',
            'max': 'max_power_kW',
            'min': 'min_power_kW'
        })
        
        meter_output = output_dir_path / "pandas_meter_summary.csv"
        meter_summary.to_csv(meter_output, index=False)
        output_paths['meter_summary'] = str(meter_output)
        logger.info(f"Pandas meter summary: {meter_output}")
        
        # 4. Zone-level aggregations
        zone_summary = df.groupby('Zone_ID').agg({
            'Active_Power_kW': ['count', 'mean', 'max'],
            'Voltage_V': 'max'
        }).reset_index()
        zone_summary.columns = ['Zone_ID', 'record_count', 'avg_power_kW', 'max_power_kW', 'max_voltage_V']
        
        zone_output = output_dir_path / "pandas_zone_summary.csv"
        zone_summary.to_csv(zone_output, index=False)
        output_paths['zone_summary'] = str(zone_output)
        logger.info(f"Pandas zone summary: {zone_output}")
        
        logger.info("Pandas processing completed")
        return output_paths
    
    def process(self, df: pd.DataFrame, output_dir: str) -> Dict[str, str]:
        """
        Main processing method - uses Spark if available, Pandas otherwise.
        
        Args:
            df: Input DataFrame
            output_dir: Output directory
            
        Returns:
            Dictionary of output paths
        """
        if self.use_spark:
            return self.process_with_spark(df, output_dir)
        else:
            return self.process_with_pandas(df, output_dir)


def main():
    """Example usage."""
    logger.info("Spark processor module loaded")


if __name__ == "__main__":
    main()
