"""
Data Ingestion Module - Raw Data Loading and Validation
Handles loading raw smart meter data from CSV files with schema validation.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    EXPECTED_COLUMNS, DATA_TYPES, RAW_DATA_DIR, 
    PROCESSED_DATA_DIR, OUTPUT_FORMAT, COMPRESSION
)
from src.utils import (
    setup_logger, log_ingestion_summary, validate_dataframe_schema,
    get_data_quality_report, get_memory_stats, parse_timestamp_safe
)

logger = setup_logger(__name__, 'ingestion.log')


class RawDataIngestion:
    """
    Handles ingestion of raw smart meter data from CSV files.
    Performs schema validation and initial data quality checks.
    """
    
    def __init__(self):
        self.ingestion_logs = []
        self.quality_reports = []
    
    def load_raw_csv(self, filepath: str, encoding: str = 'utf-8') -> Tuple[pd.DataFrame, Dict]:
        """
        Load raw CSV file with validation.
        
        Args:
            filepath: Path to raw CSV file
            encoding: File encoding
            
        Returns:
            Tuple of (DataFrame, ingestion_log)
        """
        logger.info(f"Loading raw data from: {filepath}")
        
        try:
            # Read CSV
            df = pd.read_csv(filepath, encoding=encoding)
            ingestion_timestamp = datetime.now()
            
            logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
            
            # Validate schema
            is_valid, missing_cols = validate_dataframe_schema(df, EXPECTED_COLUMNS)
            if not is_valid:
                logger.error(f"Schema validation failed. Missing columns: {missing_cols}")
                raise ValueError(f"Schema mismatch. Missing columns: {missing_cols}")
            
            # Create ingestion log
            ingestion_log = log_ingestion_summary(
                filepath, len(df), df.columns.tolist(), ingestion_timestamp
            )
            self.ingestion_logs.append(ingestion_log)
            
            logger.info("Schema validation passed")
            return df, ingestion_log
            
        except FileNotFoundError:
            logger.error(f"File not found: {filepath}")
            raise
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
            raise
    
    def parse_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse Timestamp column to datetime.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with parsed timestamps
        """
        logger.info("Parsing timestamps...")
        
        df = df.copy()
        df['Timestamp'] = df['Timestamp'].apply(parse_timestamp_safe)
        
        # Check for unparseable timestamps
        unparseable = df['Timestamp'].isna().sum()
        if unparseable > 0:
            logger.warning(f"Could not parse {unparseable} timestamps")
        
        # Sort by timestamp
        df = df.sort_values('Timestamp').reset_index(drop=True)
        logger.info("Timestamps parsed and sorted")
        
        return df
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names (strip whitespace, consistent casing).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with standardized column names
        """
        logger.info("Standardizing column names...")
        
        df = df.copy()
        df.columns = df.columns.str.strip()  # Remove leading/trailing whitespace
        logger.info("Column names standardized")
        
        return df
    
    def enforce_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enforce expected data types.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with correct data types
        """
        logger.info("Enforcing data types...")
        
        df = df.copy()
        for col, dtype in DATA_TYPES.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"Could not convert {col} to {dtype}: {str(e)}")
        
        logger.info("Data types enforced")
        return df
    
    def generate_quality_report(self, df: pd.DataFrame, stage: str = 'raw') -> Dict:
        """
        Generate initial data quality report.
        
        Args:
            df: Input DataFrame
            stage: Processing stage name
            
        Returns:
            Quality report dictionary
        """
        logger.info(f"Generating quality report for stage: {stage}")
        
        report = get_data_quality_report(df)
        report['stage'] = stage
        report['timestamp'] = datetime.now().isoformat()
        self.quality_reports.append(report)
        
        logger.info(f"Quality report generated: {report['total_rows']} rows, "
                   f"{report['duplicate_rows']} duplicates")
        
        return report
    
    def save_ingestion_metadata(self, output_path: str = None) -> None:
        """
        Save ingestion logs to file.
        
        Args:
            output_path: Optional output path
        """
        if output_path is None:
            output_path = RAW_DATA_DIR / 'ingestion_metadata.log'
        
        logger.info(f"Saving ingestion metadata to {output_path}")
        
        with open(output_path, 'w') as f:
            for log in self.ingestion_logs:
                f.write(f"{log}\n")
        
        logger.info("Ingestion metadata saved")
    
    def ingest_and_prepare(self, filepath: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Complete ingestion pipeline: load, validate, parse, standardize, type-enforce.
        
        Args:
            filepath: Path to raw data file
            
        Returns:
            Tuple of (processed_DataFrame, metadata_dict)
        """
        logger.info("=" * 80)
        logger.info("STARTING DATA INGESTION PIPELINE")
        logger.info("=" * 80)
        
        # Load raw data
        df, ingestion_log = self.load_raw_csv(filepath)
        
        # Standardize column names
        df = self.standardize_column_names(df)
        
        # Parse timestamps
        df = self.parse_timestamps(df)
        
        # Enforce data types
        df = self.enforce_data_types(df)
        
        # Generate quality report
        quality_report = self.generate_quality_report(df, stage='raw_ingested')
        
        # Memory stats
        memory_stats = get_memory_stats(df)
        
        logger.info("=" * 80)
        logger.info("INGESTION PIPELINE COMPLETED")
        logger.info(f"Final dataset: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Memory usage: {memory_stats['total_mb']} MB")
        logger.info("=" * 80)
        
        metadata = {
            'ingestion_log': ingestion_log,
            'quality_report': quality_report,
            'memory_stats': memory_stats
        }
        
        return df, metadata


def main():
    """Example usage of RawDataIngestion."""
    ingestion = RawDataIngestion()
    
    # This assumes a test CSV exists in data/raw/
    test_file = RAW_DATA_DIR / 'sample_raw_data.csv'
    
    if test_file.exists():
        df, metadata = ingestion.ingest_and_prepare(str(test_file))
        print(f"\nSuccessfully ingested {len(df)} rows")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nFirst few rows:\n{df.head()}")
    else:
        logger.info(f"Sample file not found at {test_file}")
        logger.info("Run the data generation script first to create sample data")


if __name__ == "__main__":
    main()
