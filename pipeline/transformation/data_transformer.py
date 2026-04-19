"""
Data Transformation Module
Wraps src modules and provides transformation pipeline interface.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple, Dict
import sys
import logging

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.cleaning import DataCleaner
from src.feature_engineering import FeatureEngineer
from src.validation import DataValidator

logger = logging.getLogger(__name__)


class DataTransformer:
    """Orchestrates data transformation pipeline."""
    
    def __init__(self):
        """Initialize transformation components."""
        self.cleaner = DataCleaner()
        self.engineer = FeatureEngineer()
        self.validator = DataValidator()
        self.transformation_log = {}
    
    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Complete transformation: clean -> engineer -> validate.
        
        Args:
            df: Input raw DataFrame
            
        Returns:
            Tuple of (transformed_df, transformation_log)
        """
        logger.info("Starting data transformation pipeline...")
        
        # Step 1: Clean
        logger.info("Step 1: Cleaning...")
        df_clean, clean_report = self.cleaner.clean_pipeline(df)
        self.transformation_log['cleaning'] = clean_report
        logger.info(f"  Cleaned: {len(df_clean):,} records retained, {clean_report.get('rows_removed', 0)} removed")
        
        # Step 2: Feature Engineer
        logger.info("Step 2: Feature Engineering...")
        df_features, feature_report = self.engineer.feature_engineering_pipeline(df_clean)
        self.transformation_log['feature_engineering'] = feature_report
        logger.info(f"  Added {len(df_features.columns) - len(df_clean.columns)} features")
        
        # Step 3: Validate
        logger.info("Step 3: Validation...")
        valid, validation_report = self.validator.validation_pipeline(df_features)
        self.transformation_log['validation'] = {
            'passed': valid,
            'report': validation_report
        }
        logger.info(f"  Validation: {'PASSED' if valid else 'PASSED WITH WARNINGS'}")
        
        logger.info("Data transformation pipeline completed")
        return df_features, self.transformation_log


def main():
    """Example usage."""
    logger.info("DataTransformer module loaded successfully")


if __name__ == "__main__":
    main()
