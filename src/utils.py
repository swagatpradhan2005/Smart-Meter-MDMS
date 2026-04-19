"""
Utility functions for Smart Meter MDMS Pipeline.
Logging, data validation helpers, and common operations.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any

# Import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import LOG_DIR, LOG_FORMAT, LOG_LEVEL


def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Set up logger with console and file handlers.
    
    Args:
        name: Logger name
        log_file: Optional log file path
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        log_path = LOG_DIR / log_file
        file_handler = logging.FileHandler(log_path, mode='a')
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(file_handler)
    
    return logger


def log_ingestion_summary(filepath: str, rows_loaded: int, 
                          columns: List[str], timestamp: datetime) -> Dict[str, Any]:
    """
    Create ingestion log entry.
    
    Args:
        filepath: Source file path
        rows_loaded: Number of rows loaded
        columns: Column names
        timestamp: Ingestion timestamp
        
    Returns:
        Ingestion log dictionary
    """
    return {
        'ingestion_timestamp': timestamp,
        'source_file': filepath,
        'rows_loaded': rows_loaded,
        'columns': columns,
        'column_count': len(columns),
        'status': 'success'
    }


def validate_dataframe_schema(df: pd.DataFrame, 
                             expected_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Validate DataFrame schema against expected columns.
    
    Args:
        df: Input DataFrame
        expected_columns: Expected column names
        
    Returns:
        Tuple of (is_valid, missing_columns)
    """
    missing = set(expected_columns) - set(df.columns)
    return len(missing) == 0, list(missing)


def get_data_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate data quality report.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Data quality report dictionary
    """
    report = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
        'null_counts': df.isnull().sum().to_dict(),
        'null_percentages': (df.isnull().sum() / len(df) * 100).to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'duplicate_percentage': (df.duplicated().sum() / len(df) * 100) if len(df) > 0 else 0,
        'dtypes': df.dtypes.astype(str).to_dict(),
        'numeric_stats': df.describe().to_dict() if len(df) > 0 else {}
    }
    return report


def detect_outliers_iqr(series: pd.Series, multiplier: float = 1.5) -> pd.Series:
    """
    Detect outliers using Interquartile Range (IQR) method.
    
    Args:
        series: Input pandas Series
        multiplier: IQR multiplier for threshold
        
    Returns:
        Boolean Series indicating outliers
    """
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    return (series < lower_bound) | (series > upper_bound)


def detect_outliers_zscore(series: pd.Series, threshold: float = 3) -> pd.Series:
    """
    Detect outliers using Z-score method.
    
    Args:
        series: Input pandas Series
        threshold: Z-score threshold
        
    Returns:
        Boolean Series indicating outliers
    """
    mean = series.mean()
    std = series.std()
    z_scores = np.abs((series - mean) / std)
    return z_scores > threshold


def safe_divide(numerator: pd.Series, denominator: pd.Series, 
                default: float = 0) -> pd.Series:
    """
    Safely divide two series, handling division by zero.
    
    Args:
        numerator: Numerator series
        denominator: Denominator series
        default: Default value for division by zero
        
    Returns:
        Result series
    """
    result = numerator.copy()
    mask = denominator != 0
    result[~mask] = default
    result[mask] = numerator[mask] / denominator[mask]
    return result


def parse_timestamp_safe(timestamp_str: str, 
                        formats: List[str] = None) -> pd.Timestamp or None:
    """
    Safely parse timestamp from string with multiple format support.
    
    Args:
        timestamp_str: Timestamp string
        formats: List of formats to try
        
    Returns:
        Parsed timestamp or None
    """
    if pd.isna(timestamp_str):
        return None
    
    if formats is None:
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%d/%m/%Y %H:%M:%S',
            '%Y-%m-%d',
            '%Y/%m/%d'
        ]
    
    for fmt in formats:
        try:
            return pd.to_datetime(timestamp_str, format=fmt)
        except (ValueError, TypeError):
            continue
    
    # Try pandas automatic parsing
    try:
        return pd.to_datetime(timestamp_str)
    except:
        return None


def get_memory_stats(df: pd.DataFrame) -> Dict[str, str]:
    """
    Get memory usage statistics for DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary with memory statistics
    """
    memory_bytes = df.memory_usage(deep=True).sum()
    return {
        'total_bytes': memory_bytes,
        'total_mb': f"{memory_bytes / 1024**2:.2f}",
        'total_gb': f"{memory_bytes / 1024**3:.4f}",
        'avg_bytes_per_row': memory_bytes / len(df) if len(df) > 0 else 0
    }


def sample_dataframe(df: pd.DataFrame, fraction: float = 0.1) -> pd.DataFrame:
    """
    Sample DataFrame for testing/exploration.
    
    Args:
        df: Input DataFrame
        fraction: Sampling fraction (0-1)
        
    Returns:
        Sampled DataFrame
    """
    if fraction >= 1.0:
        return df
    return df.sample(frac=fraction, random_state=42)


class DataQualityCheck:
    """Utility class for data quality checks."""
    
    @staticmethod
    def check_nulls(df: pd.DataFrame, max_percentage: float = 5.0) -> Dict[str, Any]:
        """Check null value percentages."""
        null_pct = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
        failed = null_pct[null_pct > max_percentage]
        return {
            'passed': len(failed) == 0,
            'threshold': max_percentage,
            'columns_exceeding': failed.to_dict() if len(failed) > 0 else {}
        }
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame, max_percentage: float = 1.0) -> Dict[str, Any]:
        """Check duplicate row percentages."""
        dup_pct = (df.duplicated().sum() / len(df) * 100) if len(df) > 0 else 0
        return {
            'passed': dup_pct <= max_percentage,
            'threshold': max_percentage,
            'duplicate_percentage': dup_pct,
            'duplicate_count': df.duplicated().sum()
        }
    
    @staticmethod
    def check_range_validity(df: pd.DataFrame, ranges: Dict[str, Tuple]) -> Dict[str, Any]:
        """Check if values fall within valid ranges."""
        violations = {}
        for col, (min_val, max_val) in ranges.items():
            if col in df.columns:
                out_of_range = ((df[col] < min_val) | (df[col] > max_val)).sum()
                if out_of_range > 0:
                    violations[col] = {
                        'out_of_range_count': out_of_range,
                        'valid_range': (min_val, max_val)
                    }
        return {
            'passed': len(violations) == 0,
            'violations': violations
        }
