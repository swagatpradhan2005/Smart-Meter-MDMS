"""
Storage Module - Structured Database Schema and Persistence
Handles storage of cleaned and engineered data to parquet/CSV and databases.
"""

import pandas as pd
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    PROCESSED_DATA_DIR, ANALYTICS_DATA_DIR, OUTPUT_FORMAT, COMPRESSION,
    DATABASE_CONFIG
)
from src.utils import setup_logger

logger = setup_logger(__name__, 'storage.log')


class DataStorage:
    """
    Handles data storage to multiple formats (Parquet, CSV, SQLite).
    Manages database connections and persistence layer.
    """
    
    def __init__(self):
        self.storage_log = []
        self.db_connection = None
    
    def save_to_parquet(self, df: pd.DataFrame, filename: str, 
                       directory: str = 'processed') -> str:
        """
        Save DataFrame to Parquet format (compressed).
        
        Args:
            df: Input DataFrame
            filename: Output filename (without extension)
            directory: 'processed' or 'analytics'
            
        Returns:
            Full file path
        """
        logger.info(f"Saving to Parquet: {filename}")
        
        if directory == 'processed':
            output_path = PROCESSED_DATA_DIR / f"{filename}.parquet"
        elif directory == 'analytics':
            output_path = ANALYTICS_DATA_DIR / f"{filename}.parquet"
        else:
            output_path = Path(directory) / f"{filename}.parquet"
        
        try:
            df.to_parquet(
                output_path,
                compression=COMPRESSION,
                index=False,
                engine='pyarrow'
            )
            file_size_mb = output_path.stat().st_size / 1024**2
            logger.info(f"Parquet saved: {output_path} ({file_size_mb:.2f} MB)")
            
            self.storage_log.append({
                'timestamp': datetime.now(),
                'format': 'parquet',
                'filename': str(output_path),
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': file_size_mb
            })
            
            return str(output_path)
        
        except Exception as e:
            logger.error(f"Error saving Parquet: {str(e)}")
            raise
    
    def save_to_csv(self, df: pd.DataFrame, filename: str,
                   directory: str = 'processed') -> str:
        """
        Save DataFrame to CSV format.
        
        Args:
            df: Input DataFrame
            filename: Output filename (without extension)
            directory: 'processed' or 'analytics'
            
        Returns:
            Full file path
        """
        logger.info(f"Saving to CSV: {filename}")
        
        if directory == 'processed':
            output_path = PROCESSED_DATA_DIR / f"{filename}.csv"
        elif directory == 'analytics':
            output_path = ANALYTICS_DATA_DIR / f"{filename}.csv"
        else:
            output_path = Path(directory) / f"{filename}.csv"
        
        try:
            df.to_csv(output_path, index=False)
            file_size_mb = output_path.stat().st_size / 1024**2
            logger.info(f"CSV saved: {output_path} ({file_size_mb:.2f} MB)")
            
            self.storage_log.append({
                'timestamp': datetime.now(),
                'format': 'csv',
                'filename': str(output_path),
                'rows': len(df),
                'columns': len(df.columns),
                'size_mb': file_size_mb
            })
            
            return str(output_path)
        
        except Exception as e:
            logger.error(f"Error saving CSV: {str(e)}")
            raise
    
    def create_sqlite_connection(self) -> sqlite3.Connection:
        """
        Create connection to SQLite database.
        
        Returns:
            SQLite connection object
        """
        logger.info("Creating SQLite connection...")
        
        db_path = DATABASE_CONFIG['database']
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        try:
            conn = sqlite3.connect(db_path)
            logger.info(f"SQLite connection created: {db_path}")
            self.db_connection = conn
            return conn
        except Exception as e:
            logger.error(f"Error creating SQLite connection: {str(e)}")
            raise
    
    def close_sqlite_connection(self) -> None:
        """Close SQLite connection."""
        if self.db_connection:
            self.db_connection.close()
            logger.info("SQLite connection closed")
    
    def save_to_sqlite(self, df: pd.DataFrame, table_name: str,
                      if_exists: str = 'replace') -> None:
        """
        Save DataFrame to SQLite database.
        
        Args:
            df: Input DataFrame
            table_name: Target table name
            if_exists: 'fail', 'replace', or 'append'
        """
        logger.info(f"Saving to SQLite table: {table_name} (if_exists={if_exists})")
        
        try:
            if not self.db_connection:
                self.create_sqlite_connection()
            
            df.to_sql(table_name, self.db_connection, if_exists=if_exists, index=False)
            logger.info(f"Data saved to SQLite table: {table_name}")
            
            self.storage_log.append({
                'timestamp': datetime.now(),
                'format': 'sqlite',
                'table': table_name,
                'rows': len(df),
                'columns': len(df.columns)
            })
        
        except Exception as e:
            logger.error(f"Error saving to SQLite: {str(e)}")
            raise
    
    def execute_sql(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results.
        
        Args:
            query: SQL query string
            
        Returns:
            Results as DataFrame
        """
        logger.info(f"Executing SQL query...")
        
        try:
            if not self.db_connection:
                self.create_sqlite_connection()
            
            result_df = pd.read_sql_query(query, self.db_connection)
            logger.info(f"Query returned {len(result_df)} rows")
            
            return result_df
        
        except Exception as e:
            logger.error(f"Error executing SQL: {str(e)}")
            raise
    
    def create_schema(self, schema_sql: str) -> None:
        """
        Create database schema from SQL script.
        
        Args:
            schema_sql: SQL schema definition string
        """
        logger.info("Creating database schema...")
        
        try:
            if not self.db_connection:
                self.create_sqlite_connection()
            
            # Execute schema creation
            cursor = self.db_connection.cursor()
            
            # Split by semicolon and execute each statement
            statements = schema_sql.split(';')
            for statement in statements:
                if statement.strip():
                    cursor.execute(statement)
            
            self.db_connection.commit()
            logger.info("Database schema created successfully")
        
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            self.db_connection.rollback()
            raise
    
    def get_storage_summary(self) -> Dict[str, Any]:
        """
        Get summary of storage operations.
        
        Returns:
            Storage summary dictionary
        """
        summary = {
            'total_operations': len(self.storage_log),
            'operations': self.storage_log
        }
        return summary
    
    def save_storage_log(self, output_path: str = None) -> None:
        """
        Save storage log to file.
        
        Args:
            output_path: Output file path
        """
        if output_path is None:
            output_path = PROCESSED_DATA_DIR / 'storage.log'
        
        logger.info(f"Saving storage log to {output_path}")
        
        with open(output_path, 'w') as f:
            for op in self.storage_log:
                f.write(f"{op}\n")


class AnalyticsReadyDataBuilder:
    """
    Builds analytics-ready dataset optimized for SQL queries and reporting.
    """
    
    def __init__(self):
        self.logger = setup_logger(__name__, 'analytics_builder.log')
    
    def build_analytics_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Build analytics-ready dataset with optimized columns and aggregations.
        
        Args:
            df: Input feature-engineered DataFrame
            
        Returns:
            Analytics-ready DataFrame
        """
        self.logger.info("Building analytics-ready dataset...")
        
        analytics_df = df.copy()
        
        # Select key columns for analytics
        key_columns = [
            'Timestamp', 'Meter_ID', 'Zone_ID',
            'Active_Power_kW', 'Reactive_Power_kW', 'Apparent_Power_kVA',
            'Voltage_V', 'Current_A', 'Frequency_Hz', 'Outdoor_Temp_C',
            'hour', 'day_of_week', 'month', 'season', 'peak_hour_flag',
            'power_factor', 'load_category', 'consumption_bucket',
            'is_anomaly_raw'
        ]
        
        # Keep only columns that exist
        existing_key_columns = [col for col in key_columns if col in analytics_df.columns]
        analytics_df = analytics_df[existing_key_columns]
        
        self.logger.info(f"Analytics dataset created with {len(analytics_df.columns)} columns")
        
        return analytics_df
    
    def create_aggregation_views(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Create various aggregation views for analytics.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary of aggregated views
        """
        self.logger.info("Creating aggregation views...")
        
        views = {}
        
        # Daily consumption by meter
        if 'Timestamp' in df.columns and 'Meter_ID' in df.columns and 'Active_Power_kW' in df.columns:
            daily_view = df.groupby([
                df['Timestamp'].dt.date, 'Meter_ID'
            ])['Active_Power_kW'].agg(['sum', 'mean', 'min', 'max', 'count']).reset_index()
            daily_view.columns = ['Date', 'Meter_ID', 'Total_kWh', 'Avg_kW', 'Min_kW', 'Max_kW', 'Count']
            views['daily_consumption'] = daily_view
            self.logger.info(f"Daily consumption view: {len(daily_view)} rows")
        
        # Hourly consumption by zone
        if 'Timestamp' in df.columns and 'Zone_ID' in df.columns and 'Active_Power_kW' in df.columns:
            hourly_view = df.groupby([
                df['Timestamp'].dt.floor('H'), 'Zone_ID'
            ])['Active_Power_kW'].agg(['sum', 'mean', 'count']).reset_index()
            hourly_view.columns = ['Hour', 'Zone_ID', 'Total_kWh', 'Avg_kW', 'Count']
            views['hourly_zone_consumption'] = hourly_view
            self.logger.info(f"Hourly zone consumption view: {len(hourly_view)} rows")
        
        # Peak hour analysis
        if 'peak_hour_flag' in df.columns and 'Active_Power_kW' in df.columns:
            peak_view = df.groupby('peak_hour_flag')['Active_Power_kW'].agg(['sum', 'mean', 'std', 'count']).reset_index()
            peak_view.columns = ['Is_Peak_Hour', 'Total_kWh', 'Avg_kW', 'Std_kW', 'Count']
            views['peak_hour_analysis'] = peak_view
            self.logger.info(f"Peak hour analysis view: {len(peak_view)} rows")
        
        # Load category distribution
        if 'load_category' in df.columns:
            load_view = df.groupby('load_category').size().reset_index(name='Count')
            views['load_category_distribution'] = load_view
            self.logger.info(f"Load category distribution view: {len(load_view)} rows")
        
        return views


def main():
    """Example usage of DataStorage."""
    logger.info("DataStorage module loaded successfully")


if __name__ == "__main__":
    main()
