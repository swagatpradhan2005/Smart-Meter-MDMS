"""
Feature Engineering Module - Create Analytics-Ready Features
Derives temporal, electrical, and aggregation features from raw smart meter data.
"""

import pandas as pd
import numpy as np
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.config import (
    ROLLING_WINDOW_HOURS, PEAK_HOURS, SEASONS, LOAD_CATEGORIES
)
from src.utils import setup_logger, safe_divide

logger = setup_logger(__name__, 'feature_engineering.log')


class FeatureEngineer:
    """
    Feature engineering pipeline for smart meter data.
    Creates temporal, electrical, and aggregation features.
    """
    
    def __init__(self):
        self.features_created = []
    
    def extract_temporal_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract temporal features from Timestamp column.
        
        Args:
            df: Input DataFrame with Timestamp column
            
        Returns:
            DataFrame with temporal features
        """
        logger.info("Extracting temporal features...")
        
        df = df.copy()
        df['Timestamp'] = pd.to_datetime(df['Timestamp'])
        
        # Basic temporal features
        df['hour'] = df['Timestamp'].dt.hour
        df['day_of_week'] = df['Timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['day_of_month'] = df['Timestamp'].dt.day
        df['month'] = df['Timestamp'].dt.month
        df['quarter'] = df['Timestamp'].dt.quarter
        df['year'] = df['Timestamp'].dt.year
        df['week_of_year'] = df['Timestamp'].dt.isocalendar().week
        df['day_name'] = df['Timestamp'].dt.day_name()
        
        # Cyclical encoding for hour (sine/cosine for circular nature)
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        
        # Cyclical encoding for month
        df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
        
        # Cyclical encoding for day of week
        df['dow_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
        df['dow_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
        
        logger.info("Temporal features extracted: hour, day_of_week, month, season, etc.")
        self.features_created.extend([
            'hour', 'day_of_week', 'day_of_month', 'month', 'quarter', 'year',
            'week_of_year', 'day_name', 'hour_sin', 'hour_cos', 'month_sin',
            'month_cos', 'dow_sin', 'dow_cos'
        ])
        
        return df
    
    def create_season_feature(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create season feature based on month (Indian context).
        
        Args:
            df: Input DataFrame with month column
            
        Returns:
            DataFrame with season feature
        """
        logger.info("Creating season feature...")
        
        df = df.copy()
        
        def get_season(month):
            for season, months in SEASONS.items():
                if month in months:
                    return season
            return 'Unknown'
        
        df['season'] = df['month'].apply(get_season)
        
        logger.info(f"Season feature created with categories: {df['season'].unique().tolist()}")
        self.features_created.append('season')
        
        return df
    
    def create_peak_hour_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create peak hour flag (10 AM to 10 PM for Indian context).
        
        Args:
            df: Input DataFrame with hour column
            
        Returns:
            DataFrame with peak_hour_flag
        """
        logger.info(f"Creating peak hour flag (peak hours: {PEAK_HOURS})...")
        
        df = df.copy()
        df['peak_hour_flag'] = df['hour'].isin(PEAK_HOURS).astype(int)
        
        peak_rows = df['peak_hour_flag'].sum()
        logger.info(f"Peak hour flag created: {peak_rows} peak hour rows out of {len(df)}")
        self.features_created.append('peak_hour_flag')
        
        return df
    
    def calculate_power_factor(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate power factor: cos(angle) = Active_Power / Apparent_Power.
        Power factor = [0, 1], where 1 is ideal (purely resistive load).
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with power_factor
        """
        logger.info("Calculating power factor...")
        
        df = df.copy()
        
        if 'Active_Power_kW' in df.columns and 'Apparent_Power_kVA' in df.columns:
            df['power_factor'] = safe_divide(
                df['Active_Power_kW'],
                df['Apparent_Power_kVA'],
                default=0.0
            )
            # Clamp to [0, 1]
            df['power_factor'] = df['power_factor'].clip(0, 1)
            
            logger.info(f"Power factor calculated. Range: [{df['power_factor'].min():.4f}, "
                       f"{df['power_factor'].max():.4f}]")
            self.features_created.append('power_factor')
        else:
            logger.warning("Required columns for power factor calculation not found")
        
        return df
    
    def create_load_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create load category based on Active_Power_kW.
        Categories: Low, Medium, High, VeryHigh
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with load_category
        """
        logger.info("Creating load category...")
        
        df = df.copy()
        
        def categorize_load(power):
            for category, (min_val, max_val) in LOAD_CATEGORIES.items():
                if min_val <= power < max_val:
                    return category
            return 'VeryHigh'
        
        df['load_category'] = df['Active_Power_kW'].apply(categorize_load)
        
        logger.info(f"Load categories created:")
        for category in df['load_category'].unique():
            count = (df['load_category'] == category).sum()
            logger.info(f"  {category}: {count} rows")
        
        self.features_created.append('load_category')
        return df
    
    def create_rolling_features(self, df: pd.DataFrame, 
                               window_hours: int = None,
                               columns: List[str] = None) -> pd.DataFrame:
        """
        Create rolling average and rolling delta features.
        
        Args:
            df: Input DataFrame (must be sorted by Timestamp)
            window_hours: Rolling window size in hours
            columns: Columns to compute rolling features for
            
        Returns:
            DataFrame with rolling features
        """
        logger.info(f"Creating rolling features (window: {window_hours or ROLLING_WINDOW_HOURS}h)...")
        
        if window_hours is None:
            window_hours = ROLLING_WINDOW_HOURS
        
        if columns is None:
            columns = ['Active_Power_kW', 'Current_A', 'Reactive_Power_kW']
        
        df = df.copy()
        
        # Ensure data is sorted by timestamp
        df = df.sort_values('Timestamp').reset_index(drop=True)
        
        for col in columns:
            if col in df.columns:
                # Rolling mean
                df[f'{col}_rolling_mean_{window_hours}h'] = df.groupby('Meter_ID')[col].transform(
                    lambda x: x.rolling(window=window_hours, min_periods=1).mean()
                )
                
                # Rolling std
                df[f'{col}_rolling_std_{window_hours}h'] = df.groupby('Meter_ID')[col].transform(
                    lambda x: x.rolling(window=window_hours, min_periods=1).std()
                )
                
                # Rolling delta (difference from rolling mean)
                df[f'{col}_delta_from_mean_{window_hours}h'] = (
                    df[col] - df[f'{col}_rolling_mean_{window_hours}h']
                )
                
                logger.info(f"  Created rolling features for {col}")
                self.features_created.extend([
                    f'{col}_rolling_mean_{window_hours}h',
                    f'{col}_rolling_std_{window_hours}h',
                    f'{col}_delta_from_mean_{window_hours}h'
                ])
        
        return df
    
    def create_consumption_bucket(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create consumption bucket (quartile) for consumption distribution analysis.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with consumption_bucket
        """
        logger.info("Creating consumption bucket...")
        
        df = df.copy()
        
        df['consumption_bucket'] = pd.qcut(
            df['Active_Power_kW'],
            q=4,
            labels=['Q1_Low', 'Q2_Medium', 'Q3_High', 'Q4_VeryHigh'],
            duplicates='drop'
        )
        
        logger.info(f"Consumption buckets created:")
        for bucket in df['consumption_bucket'].unique():
            count = (df['consumption_bucket'] == bucket).sum()
            logger.info(f"  {bucket}: {count} rows")
        
        self.features_created.append('consumption_bucket')
        return df
    
    def create_aggregation_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create meter-level and zone-level aggregation features.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with aggregation features
        """
        logger.info("Creating aggregation features...")
        
        df = df.copy()
        
        # Meter-level daily consumption
        df['meter_daily_consumption_kWh'] = df.groupby(
            ['Meter_ID', df['Timestamp'].dt.date]
        )['Active_Power_kW'].transform('sum')
        
        # Zone-level hourly consumption
        df['zone_hourly_consumption_kWh'] = df.groupby(
            ['Zone_ID', df['Timestamp'].dt.floor('h')]
        )['Active_Power_kW'].transform('sum')
        
        # Meter-level average power
        df['meter_avg_power_kW'] = df.groupby('Meter_ID')['Active_Power_kW'].transform('mean')
        
        # Zone-level average power
        df['zone_avg_power_kW'] = df.groupby('Zone_ID')['Active_Power_kW'].transform('mean')
        
        logger.info("Aggregation features created")
        self.features_created.extend([
            'meter_daily_consumption_kWh',
            'zone_hourly_consumption_kWh',
            'meter_avg_power_kW',
            'zone_avg_power_kW'
        ])
        
        return df
    
    def feature_engineering_pipeline(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Complete feature engineering pipeline.
        
        Args:
            df: Input cleaned DataFrame
            
        Returns:
            Tuple of (features_DataFrame, feature_report)
        """
        logger.info("=" * 80)
        logger.info("STARTING FEATURE ENGINEERING PIPELINE")
        logger.info("=" * 80)
        logger.info(f"Initial columns: {len(df.columns)}")
        
        df = df.copy()
        
        # Extract temporal features
        df = self.extract_temporal_features(df)
        
        # Create season
        df = self.create_season_feature(df)
        
        # Create peak hour flag
        df = self.create_peak_hour_flag(df)
        
        # Calculate power factor
        df = self.calculate_power_factor(df)
        
        # Create load category
        df = self.create_load_category(df)
        
        # Create rolling features
        df = self.create_rolling_features(df)
        
        # Create consumption bucket
        df = self.create_consumption_bucket(df)
        
        # Create aggregation features
        df = self.create_aggregation_features(df)
        
        feature_report = {
            'features_created': self.features_created,
            'total_features': len(self.features_created),
            'final_columns': len(df.columns),
            'column_list': df.columns.tolist()
        }
        
        logger.info("=" * 80)
        logger.info("FEATURE ENGINEERING PIPELINE COMPLETED")
        logger.info(f"Final columns: {len(df.columns)}")
        logger.info(f"Features created: {len(self.features_created)}")
        logger.info(f"New features: {self.features_created}")
        logger.info("=" * 80)
        
        return df, feature_report


def main():
    """Example usage of FeatureEngineer."""
    logger.info("FeatureEngineer module loaded successfully")


if __name__ == "__main__":
    main()
