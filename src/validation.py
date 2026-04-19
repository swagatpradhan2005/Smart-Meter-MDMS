"""
Data Validation Module - Quality Checks and Validation Rules
Comprehensive data quality assessment before analytics.
"""

import pandas as pd
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import DATA_QUALITY_THRESHOLDS, VALID_RANGES
from src.utils import setup_logger, DataQualityCheck

logger = setup_logger(__name__, 'validation.log')


class DataValidator:
    """
    Comprehensive data validation pipeline.
    Ensures data quality meets business rules before analytics.
    """
    
    def __init__(self):
        self.validation_results = {}
        self.validation_passed = True
    
    def check_completeness(self, df: pd.DataFrame, 
                          threshold: float = None) -> Dict[str, Any]:
        """
        Check data completeness (null value percentages).
        
        Args:
            df: Input DataFrame
            threshold: Maximum allowable null percentage
            
        Returns:
            Validation result dictionary
        """
        logger.info("Checking data completeness...")
        
        if threshold is None:
            threshold = DATA_QUALITY_THRESHOLDS['max_null_percentage']
        
        check_result = DataQualityCheck.check_nulls(df, threshold)
        
        logger.info(f"Completeness check: {'PASSED' if check_result['passed'] else 'FAILED'}")
        if not check_result['passed']:
            logger.warning(f"Columns exceeding {threshold}% threshold: "
                          f"{check_result['columns_exceeding']}")
            self.validation_passed = False
        
        self.validation_results['completeness'] = check_result
        return check_result
    
    def check_uniqueness(self, df: pd.DataFrame, 
                        threshold: float = None) -> Dict[str, Any]:
        """
        Check for duplicate rows.
        
        Args:
            df: Input DataFrame
            threshold: Maximum allowable duplicate percentage
            
        Returns:
            Validation result dictionary
        """
        logger.info("Checking uniqueness...")
        
        if threshold is None:
            threshold = DATA_QUALITY_THRESHOLDS['max_duplicates_percentage']
        
        check_result = DataQualityCheck.check_duplicates(df, threshold)
        
        logger.info(f"Uniqueness check: {'PASSED' if check_result['passed'] else 'FAILED'}")
        if not check_result['passed']:
            logger.warning(f"Duplicate percentage {check_result['duplicate_percentage']:.2f}% "
                          f"exceeds threshold {threshold}%")
            self.validation_passed = False
        
        self.validation_results['uniqueness'] = check_result
        return check_result
    
    def check_validity(self, df: pd.DataFrame, 
                      valid_ranges: Dict = None) -> Dict[str, Any]:
        """
        Check if values fall within valid ranges.
        
        Args:
            df: Input DataFrame
            valid_ranges: Dictionary of {column: (min, max)}
            
        Returns:
            Validation result dictionary
        """
        logger.info("Checking value validity...")
        
        if valid_ranges is None:
            valid_ranges = VALID_RANGES
        
        check_result = DataQualityCheck.check_range_validity(df, valid_ranges)
        
        logger.info(f"Validity check: {'PASSED' if check_result['passed'] else 'FAILED'}")
        if not check_result['passed']:
            logger.warning(f"Violations detected: {check_result['violations']}")
            self.validation_passed = False
        
        self.validation_results['validity'] = check_result
        return check_result
    
    def check_consistency(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Check data consistency and logical rules.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Validation result dictionary
        """
        logger.info("Checking consistency...")
        
        issues = []
        
        # Check: All required columns present
        required_cols = ['Timestamp', 'Meter_ID', 'Zone_ID', 'Active_Power_kW']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
        
        # Check: Timestamp uniqueness per meter (should not have exact duplicates)
        if 'Timestamp' in df.columns and 'Meter_ID' in df.columns:
            duplicates = df.groupby(['Meter_ID', 'Timestamp']).size()
            if (duplicates > 1).any():
                dup_count = (duplicates > 1).sum()
                issues.append(f"Duplicate (Meter_ID, Timestamp) combinations: {dup_count}")
        
        # Check: Timestamp sequence is continuous or expected gaps
        if 'Timestamp' in df.columns:
            df_sorted = df.sort_values('Timestamp')
            time_diffs = df_sorted['Timestamp'].diff().dropna()
            # Most intervals should be consistent (typically 15 or 30 minutes)
            common_interval = time_diffs.mode()
            if len(common_interval) > 0:
                logger.info(f"Most common time interval: {common_interval[0]}")
        
        # Check: No future dates
        if 'Timestamp' in df.columns:
            future_dates = (df['Timestamp'] > pd.Timestamp.now()).sum()
            if future_dates > 0:
                issues.append(f"Future dates detected: {future_dates} rows")
        
        consistency_passed = len(issues) == 0
        
        logger.info(f"Consistency check: {'PASSED' if consistency_passed else 'FAILED'}")
        if not consistency_passed:
            for issue in issues:
                logger.warning(f"  - {issue}")
            self.validation_passed = False
        
        check_result = {
            'passed': consistency_passed,
            'issues': issues
        }
        self.validation_results['consistency'] = check_result
        return check_result
    
    def check_anomalies(self, df: pd.DataFrame, 
                       threshold: float = None) -> Dict[str, Any]:
        """
        Check anomaly/outlier percentages.
        
        Args:
            df: Input DataFrame
            threshold: Maximum allowable anomaly percentage
            
        Returns:
            Validation result dictionary
        """
        logger.info("Checking anomalies...")
        
        if threshold is None:
            threshold = DATA_QUALITY_THRESHOLDS['max_anomalies_percentage']
        
        if 'is_anomaly_raw' in df.columns:
            anomaly_count = df['is_anomaly_raw'].sum()
            anomaly_pct = (anomaly_count / len(df)) * 100 if len(df) > 0 else 0
            
            passed = anomaly_pct <= threshold
            logger.info(f"Anomaly check: {'PASSED' if passed else 'FAILED'}")
            logger.info(f"Anomalies: {anomaly_count} ({anomaly_pct:.2f}%)")
            
            if not passed:
                logger.warning(f"Anomaly percentage {anomaly_pct:.2f}% exceeds threshold {threshold}%")
                self.validation_passed = False
            
            check_result = {
                'passed': passed,
                'threshold': threshold,
                'anomaly_count': anomaly_count,
                'anomaly_percentage': anomaly_pct
            }
        else:
            logger.info("Anomaly column not found, skipping")
            check_result = {
                'passed': True,
                'skipped': True,
                'reason': 'is_anomaly_raw column not found'
            }
        
        self.validation_results['anomalies'] = check_result
        return check_result
    
    def generate_data_lineage(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate data lineage and provenance information.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Data lineage dictionary
        """
        logger.info("Generating data lineage...")
        
        lineage = {
            'timestamp': datetime.now().isoformat(),
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'date_range': {
                'start': str(df['Timestamp'].min()) if 'Timestamp' in df.columns else 'N/A',
                'end': str(df['Timestamp'].max()) if 'Timestamp' in df.columns else 'N/A'
            },
            'meter_ids': df['Meter_ID'].nunique() if 'Meter_ID' in df.columns else 0,
            'zone_ids': df['Zone_ID'].nunique() if 'Zone_ID' in df.columns else 0
        }
        
        return lineage
    
    def validation_pipeline(self, df: pd.DataFrame) -> Tuple[bool, Dict]:
        """
        Complete data validation pipeline.
        
        Args:
            df: Input DataFrame to validate
            
        Returns:
            Tuple of (validation_passed, validation_report)
        """
        logger.info("=" * 80)
        logger.info("STARTING DATA VALIDATION PIPELINE")
        logger.info("=" * 80)
        
        df = df.copy()
        self.validation_passed = True
        
        # Run all checks
        self.check_completeness(df)
        self.check_uniqueness(df)
        self.check_validity(df)
        self.check_consistency(df)
        self.check_anomalies(df)
        
        # Generate lineage
        lineage = self.generate_data_lineage(df)
        
        validation_report = {
            'validation_timestamp': datetime.now().isoformat(),
            'overall_status': 'PASSED' if self.validation_passed else 'FAILED',
            'validation_results': self.validation_results,
            'data_lineage': lineage
        }
        
        logger.info("=" * 80)
        logger.info(f"VALIDATION PIPELINE COMPLETED: {validation_report['overall_status']}")
        logger.info("=" * 80)
        
        return self.validation_passed, validation_report


def main():
    """Example usage of DataValidator."""
    logger.info("DataValidator module loaded successfully")


if __name__ == "__main__":
    main()
