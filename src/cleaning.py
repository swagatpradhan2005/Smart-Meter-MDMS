"""
Data Cleaning Module - Handle Missing Values, Duplicates, Outliers
Comprehensive data quality improvements and standardization.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    VALID_RANGES, MISSING_VALUE_STRATEGY, ANOMALY_DETECTION_METHOD,
    ANOMALY_THRESHOLD
)
from src.utils import (
    setup_logger, detect_outliers_iqr, detect_outliers_zscore,
    get_data_quality_report
)

logger = setup_logger(__name__, 'cleaning.log')


class DataCleaner:
    """
    Comprehensive data cleaning pipeline for smart meter data.
    Handles missing values, duplicates, outliers, and inconsistencies.
    """
    
    def __init__(self):
        self.cleaning_report = {}
        self.cleaning_steps = []
    
    def remove_duplicates(self, df: pd.DataFrame, 
                         subset: List[str] = None) -> pd.DataFrame:
        """
        Remove duplicate rows using composite key (Meter_ID, Timestamp).
        
        Args:
            df: Input DataFrame
            subset: Columns to consider for duplicates. Default: [Meter_ID, Timestamp]
            
        Returns:
            DataFrame with duplicates removed
        """
        logger.info("Removing duplicates using composite key (Meter_ID, Timestamp)...")
        
        initial_rows = len(df)
        df = df.copy()
        
        # Use composite key if not specified
        if subset is None:
            subset = ['Meter_ID', 'Timestamp']
        
        # Remove duplicates keeping first occurrence
        df = df.drop_duplicates(subset=subset, keep='first')
        
        duplicates_removed = initial_rows - len(df)
        logger.info(f"Removed {duplicates_removed} duplicate rows based on composite key {subset}")
        self.cleaning_steps.append({
            'step': 'remove_duplicates',
            'duplicate_key': subset,
            'rows_removed': duplicates_removed
        })
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Handle missing values per meter using FFill, BFill, and median imputation.
        Ensures missing values are handled within each meter's time series.
        
        Args:
            df: Input DataFrame (should be sorted by Meter_ID, Timestamp)
            
        Returns:
            Tuple of (cleaned_DataFrame, missing_values_report)
        """
        logger.info("Handling missing values per Meter_ID...")
        
        df = df.copy()
        
        # Sort by Meter_ID and Timestamp for proper time-series filling
        if 'Meter_ID' in df.columns and 'Timestamp' in df.columns:
            df = df.sort_values(['Meter_ID', 'Timestamp']).reset_index(drop=True)
            logger.info("Sorted data by (Meter_ID, Timestamp) for per-meter imputation")
        
        missing_report = {}
        
        # Get numeric columns only
        numeric_cols = df.select_dtypes(include=['float32', 'float64', 'int32', 'int64']).columns.tolist()
        
        # Skip Meter_ID, Timestamp, Zone_ID from numeric operations
        numeric_cols = [col for col in numeric_cols if col not in ['Meter_ID', 'Timestamp', 'Zone_ID']]
        
        for col in numeric_cols:
            initial_missing = df[col].isna().sum()
            
            if initial_missing == 0:
                continue
            
            missing_pct = (initial_missing / len(df)) * 100
            logger.info(f"Column '{col}': {initial_missing} missing values ({missing_pct:.2f}%)")
            
            # Apply per-meter forward fill and backward fill
            if 'Meter_ID' in df.columns:
                df[col] = df.groupby('Meter_ID')[col].transform(
                    lambda x: x.ffill().bfill()
                )
            else:
                # Fallback to global forward/backward fill
                df[col] = df[col].ffill().bfill()
            
            # For remaining nulls, use median
            remaining = df[col].isna().sum()
            if remaining > 0:
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                logger.info(f"  Filled remaining {remaining} values with global median: {median_val:.2f}")
            
            final_missing = df[col].isna().sum()
            missing_report[col] = {
                'initial_missing': initial_missing,
                'final_missing': final_missing,
                'missing_percentage': (initial_missing / len(df)) * 100 if len(df) > 0 else 0
            }
        
        logger.info("Per-meter missing value imputation completed")
        return df, missing_report
    
    def detect_and_flag_outliers(self, df: pd.DataFrame, 
                                columns: List[str] = None) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Detect outliers using IQR method and flag them.
        
        Args:
            df: Input DataFrame
            columns: Columns to check for outliers
            
        Returns:
            Tuple of (DataFrame with anomaly flags, anomaly_series)
        """
        logger.info("Detecting outliers using IQR method...")
        
        df = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=['float32', 'float64']).columns.tolist()
        
        anomaly_flags = pd.Series(False, index=df.index)
        outlier_counts = {}
        
        for col in columns:
            if col in df.columns:
                col_anomalies = detect_outliers_iqr(df[col], ANOMALY_THRESHOLD)
                outlier_counts[col] = col_anomalies.sum()
                anomaly_flags = anomaly_flags | col_anomalies
                
                if col_anomalies.sum() > 0:
                    logger.info(f"  Column '{col}': {col_anomalies.sum()} outliers detected")
        
        self.cleaning_steps.append({
            'step': 'detect_outliers',
            'outlier_counts': outlier_counts,
            'total_anomaly_rows': anomaly_flags.sum()
        })
        
        logger.info(f"Total anomaly rows: {anomaly_flags.sum()}")
        
        return df, anomaly_flags
    
    def clip_and_fix_invalid_values(self, df: pd.DataFrame, 
                                     valid_ranges: Dict = None) -> Tuple[pd.DataFrame, Dict]:
        """
        Clip or fix values that fall outside valid ranges.
        For ranges like Voltage (190-250V), clip to boundaries.
        For ranges like Current (0+), remove negative values.
        
        Args:
            df: Input DataFrame
            valid_ranges: Dictionary of {column: (min, max)}
            
        Returns:
            Tuple of (DataFrame with clipped values, fix_report)
        """
        logger.info("Clipping and fixing out-of-range values...")
        
        if valid_ranges is None:
            valid_ranges = VALID_RANGES
        
        df = df.copy()
        fix_report = {}
        rows_removed = 0
        
        for col, (min_val, max_val) in valid_ranges.items():
            if col not in df.columns:
                continue
            
            initial_count = len(df)
            
            # For non-negative columns (Current, Power, etc.), remove negative rows
            if min_val >= 0 and col in ['Current_A', 'Active_Power_kW', 'Reactive_Power_kW', 
                                        'Apparent_Power_kVA', 'Sub_Meter_Kitchen', 'Sub_Meter_HVAC']:
                rows_before = len(df)
                df = df[df[col] >= min_val]  # Remove negative rows
                rows_dropped = rows_before - len(df)
                if rows_dropped > 0:
                    logger.info(f"  {col}: Removed {rows_dropped} rows with negative values")
                    fix_report[col] = {
                        'action': 'removed_negative',
                        'rows_removed': rows_dropped
                    }
                    rows_removed += rows_dropped
                    continue
            
            # For other columns, clip to valid range
            before_min = (df[col] < min_val).sum()
            before_max = (df[col] > max_val).sum()
            
            df[col] = df[col].clip(lower=min_val, upper=max_val)
            
            after_min = (df[col] < min_val).sum()
            after_max = (df[col] > max_val).sum()
            
            if before_min > 0 or before_max > 0:
                logger.info(f"  {col}: Clipped {before_min + before_max} out-of-range values to [{min_val}, {max_val}]")
                fix_report[col] = {
                    'action': 'clipped',
                    'valid_range': (min_val, max_val),
                    'clipped_low': before_min,
                    'clipped_high': before_max,
                    'total_clipped': before_min + before_max
                }
        
        logger.info(f"Fixed/clipped values. Rows removed: {rows_removed}, remaining: {len(df)}")
        return df, fix_report
    
    def detect_logical_inconsistencies(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Detect logical inconsistencies in smart meter data.
        E.g., Apparent Power should equal sqrt(Active^2 + Reactive^2)
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (DataFrame with flags, inconsistency_report)
        """
        logger.info("Detecting logical inconsistencies...")
        
        df = df.copy()
        inconsistencies = {}
        
        # Check: Apparent Power = sqrt(Active^2 + Reactive^2)
        if all(col in df.columns for col in ['Active_Power_kW', 'Reactive_Power_kW', 'Apparent_Power_kVA']):
            calculated_apparent = np.sqrt(
                df['Active_Power_kW']**2 + df['Reactive_Power_kW']**2
            )
            tolerance = 0.1  # 0.1 kVA tolerance
            mismatch = (np.abs(df['Apparent_Power_kVA'] - calculated_apparent) > tolerance).sum()
            
            if mismatch > 0:
                logger.info(f"  Apparent Power mismatch: {mismatch} rows")
                inconsistencies['apparent_power_mismatch'] = mismatch
        
        # Check: Voltage should not be zero if Current is non-zero
        if all(col in df.columns for col in ['Voltage_V', 'Current_A']):
            zero_voltage_with_current = ((df['Voltage_V'] == 0) & (df['Current_A'] > 0)).sum()
            if zero_voltage_with_current > 0:
                logger.info(f"  Zero voltage with non-zero current: {zero_voltage_with_current} rows")
                inconsistencies['zero_voltage_with_current'] = zero_voltage_with_current
        
        # Check: Meter readings should be monotonically non-decreasing (if cumulative)
        # This is optional and depends on data model
        
        logger.info(f"Logical inconsistencies detected: {len(inconsistencies)}")
        return df, inconsistencies
    
    def clean_pipeline(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Complete data cleaning pipeline.
        Steps:
        1. Remove duplicates by composite key (Meter_ID, Timestamp)
        2. Handle missing values per meter
        3. Clip/fix out-of-range values
        4. Detect logical inconsistencies
        5. Flag anomalies
        
        Args:
            df: Raw input DataFrame
            
        Returns:
            Tuple of (cleaned_DataFrame, cleaning_report)
        """
        logger.info("=" * 80)
        logger.info("STARTING DATA CLEANING PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Initial dataset: {len(df):,} rows, {len(df.columns)} columns")
        
        df = df.copy()
        initial_rows = len(df)
        
        # Step 1: Remove duplicates by composite key
        df = self.remove_duplicates(df, subset=['Meter_ID', 'Timestamp'])
        logger.info(f"After deduplication: {len(df):,} rows")
        
        # Step 2: Handle missing values (per-meter)
        df, missing_report = self.handle_missing_values(df)
        logger.info(f"Missing values handled")
        
        # Step 3: Clip/fix out-of-range values
        df, fix_report = self.clip_and_fix_invalid_values(df)
        logger.info(f"After clipping invalid values: {len(df):,} rows")
        
        # Step 4: Detect logical inconsistencies
        df, inconsistency_report = self.detect_logical_inconsistencies(df)
        
        # Step 5: Detect and flag outliers (keep them, just flag)
        df, anomaly_flags = self.detect_and_flag_outliers(df)
        df['is_anomaly'] = anomaly_flags
        
        # Create comprehensive cleaning report
        self.cleaning_report = {
            'initial_rows': initial_rows,
            'final_rows': len(df),
            'rows_removed': initial_rows - len(df),
            'duplicate_rows_removed': initial_rows - len(self.remove_duplicates(df.copy(), subset=['Meter_ID', 'Timestamp'])),
            'missing_values_report': missing_report,
            'range_fixes': fix_report,
            'logical_inconsistencies': inconsistency_report,
            'anomaly_flags_count': anomaly_flags.sum(),
            'anomaly_flags_percentage': (anomaly_flags.sum() / len(df) * 100) if len(df) > 0 else 0,
            'cleaning_steps': self.cleaning_steps
        }
        
        logger.info("=" * 80)
        logger.info("DATA CLEANING PIPELINE COMPLETED")
        logger.info(f"Final dataset: {len(df):,} rows (removed {initial_rows - len(df):,} rows)")
        logger.info(f"Anomalies flagged: {anomaly_flags.sum():,} ({(anomaly_flags.sum()/len(df)*100):.2f}%)")
        logger.info("=" * 80)
        
        return df, self.cleaning_report


def main():
    """Example usage of DataCleaner."""
    logger.info("DataCleaner module loaded successfully")


if __name__ == "__main__":
    main()
