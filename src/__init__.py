"""
Smart Meter Data Management System (MDMS) - Package initialization
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"
__description__ = "Smart Meter MDMS - Data ingestion, processing, and analytics platform"

from src.ingestion import RawDataIngestion
from src.cleaning import DataCleaner
from src.feature_engineering import FeatureEngineer
from src.validation import DataValidator
from src.storage import DataStorage, AnalyticsReadyDataBuilder

__all__ = [
    'RawDataIngestion',
    'DataCleaner',
    'FeatureEngineer',
    'DataValidator',
    'DataStorage',
    'AnalyticsReadyDataBuilder'
]
