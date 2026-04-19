"""
Smart Meter Analytics Engine
Generates actionable insights: peak hours, zone-wise consumption, anomalies, trends
"""

import logging
from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """Analytics engine for smart meter insights."""
    
    def __init__(self, output_dir: str = "outputs/reports"):
        """
        Initialize analytics engine.
        
        Args:
            output_dir: Directory to save analytics reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.df = None
        self.logger = logger
    
    def load_data(self, data_file: str) -> bool:
        """Load data file."""
        try:
            self.logger.info(f"Loading data from {data_file}...")
            self.df = pd.read_csv(data_file)
            self.logger.info(f"Loaded {len(self.df)} records")
            return True
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            return False
    
    def identify_peak_hours(self) -> Dict:
        """Identify peak consumption hours."""
        try:
            self.logger.info("Identifying peak hours...")
            
            # Parse timestamp if needed
            if 'Timestamp' in self.df.columns and self.df['Timestamp'].dtype == 'object':
                self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'], errors='coerce')
            
            if 'hour' not in self.df.columns:
                self.df['hour'] = self.df['Timestamp'].dt.hour
            
            hourly = self.df.groupby('hour')['Active_Power_kW'].agg(['mean', 'std', 'min', 'max', 'count']).reset_index()
            hourly = hourly.sort_values('mean', ascending=False)
            
            results = {
                'peak_hour': int(hourly.iloc[0]['hour']),
                'peak_avg_power': float(hourly.iloc[0]['mean']),
                'off_peak_hour': int(hourly.iloc[-1]['hour']),
                'off_peak_avg_power': float(hourly.iloc[-1]['mean']),
                'top_5_peak_hours': hourly.head(5)[['hour', 'mean']].to_dict('records'),
                'hourly_summary': hourly.to_dict('records')
            }
            
            self.logger.info(f"Peak hour: Hour {results['peak_hour']} ({results['peak_avg_power']:.2f} kW avg)")
            return results
        
        except Exception as e:
            self.logger.error(f"Error in peak hours analysis: {e}")
            return {}
    
    def zone_wise_analysis(self) -> Dict:
        """Analyze consumption by zone."""
        try:
            self.logger.info("Performing zone-wise analysis...")
            
            if 'Zone' not in self.df.columns:
                self.logger.warning("Zone column not found")
                return {}
            
            zone_stats = self.df.groupby('Zone')['Active_Power_kW'].agg([
                'count', 'mean', 'std', 'min', 'max',
                ('sum', 'sum'),
                ('q25', lambda x: x.quantile(0.25)),
                ('q75', lambda x: x.quantile(0.75))
            ]).reset_index()
            
            zone_stats.columns = ['Zone', 'count', 'mean', 'std', 'min', 'max', 'total', 'q25', 'q75']
            zone_stats = zone_stats.sort_values('mean', ascending=False)
            
            results = {
                'zone_summary': zone_stats.to_dict('records'),
                'highest_consumption_zone': {
                    'zone': zone_stats.iloc[0]['Zone'],
                    'avg_power': float(zone_stats.iloc[0]['mean'])
                },
                'lowest_consumption_zone': {
                    'zone': zone_stats.iloc[-1]['Zone'],
                    'avg_power': float(zone_stats.iloc[-1]['mean'])
                }
            }
            
            self.logger.info(f"Highest zone: {results['highest_consumption_zone']['zone']}")
            return results
        
        except Exception as e:
            self.logger.error(f"Error in zone analysis: {e}")
            return {}
    
    def top_consumers(self, top_n: int = 20) -> Dict:
        """Identify top consuming meters."""
        try:
            self.logger.info(f"Identifying top {top_n} consumers...")
            
            if 'Meter_ID' not in self.df.columns:
                self.logger.warning("Meter_ID column not found")
                return {}
            
            meter_stats = self.df.groupby('Meter_ID')['Active_Power_kW'].agg([
                'count', 'mean', 'std', 'min', 'max'
            ]).reset_index()
            
            meter_stats.columns = ['Meter_ID', 'records', 'mean', 'std', 'min', 'max']
            meter_stats = meter_stats.sort_values('mean', ascending=False).head(top_n)
            
            # Get zone info if available
            if 'Zone' in self.df.columns:
                zone_map = self.df.groupby('Meter_ID')['Zone'].first()
                meter_stats['Zone'] = meter_stats['Meter_ID'].map(zone_map)
            
            results = {
                'top_consumers': meter_stats.to_dict('records'),
                'total_top_consumption': float(meter_stats['mean'].sum())
            }
            
            return results
        
        except Exception as e:
            self.logger.error(f"Error in top consumers: {e}")
            return {}
    
    def anomaly_detection(self) -> Dict:
        """Detect anomalies using IQR method."""
        try:
            self.logger.info("Detecting anomalies...")
            
            Q1 = self.df['Active_Power_kW'].quantile(0.25)
            Q3 = self.df['Active_Power_kW'].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            anomalies = self.df[(self.df['Active_Power_kW'] < lower_bound) | 
                               (self.df['Active_Power_kW'] > upper_bound)]
            
            results = {
                'total_anomalies': len(anomalies),
                'anomaly_rate': (len(anomalies) / len(self.df)) * 100,
                'detection_method': 'IQR (1.5x)',
                'bounds': {
                    'lower_bound': float(lower_bound),
                    'upper_bound': float(upper_bound),
                    'Q1': float(Q1),
                    'Q3': float(Q3),
                    'IQR': float(IQR)
                },
                'anomaly_stats': {
                    'mean': float(anomalies['Active_Power_kW'].mean()) if len(anomalies) > 0 else 0,
                    'min': float(anomalies['Active_Power_kW'].min()) if len(anomalies) > 0 else 0,
                    'max': float(anomalies['Active_Power_kW'].max()) if len(anomalies) > 0 else 0
                }
            }
            
            # Zone-wise anomalies
            if 'Zone' in self.df.columns:
                zone_anomalies = anomalies.groupby('Zone').size().reset_index(name='count')
                zone_anomalies = zone_anomalies.sort_values('count', ascending=False)
                results['anomalies_by_zone'] = zone_anomalies.to_dict('records')
            
            self.logger.info(f"Detected {len(anomalies)} anomalies ({results['anomaly_rate']:.2f}%)")
            return results
        
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return {}
    
    def seasonal_analysis(self) -> Dict:
        """Analyze seasonal trends."""
        try:
            self.logger.info("Analyzing seasonal trends...")
            
            if 'Timestamp' in self.df.columns and self.df['Timestamp'].dtype == 'object':
                self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'], errors='coerce')
            
            if 'month' not in self.df.columns:
                self.df['month'] = self.df['Timestamp'].dt.month
            
            if 'quarter' not in self.df.columns:
                self.df['quarter'] = self.df['Timestamp'].dt.quarter
            
            monthly = self.df.groupby('month')['Active_Power_kW'].agg(['mean', 'std']).reset_index()
            quarterly = self.df.groupby('quarter')['Active_Power_kW'].agg(['mean', 'std']).reset_index()
            
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            monthly['month_name'] = monthly['month'].apply(lambda x: month_names[x-1] if 1 <= x <= 12 else '')
            
            results = {
                'monthly_trend': monthly.to_dict('records'),
                'quarterly_trend': quarterly.to_dict('records'),
                'highest_consumption_month': {
                    'month': month_names[monthly.loc[monthly['mean'].idxmax(), 'month']-1],
                    'avg_power': float(monthly['mean'].max())
                },
                'lowest_consumption_month': {
                    'month': month_names[monthly.loc[monthly['mean'].idxmin(), 'month']-1],
                    'avg_power': float(monthly['mean'].min())
                }
            }
            
            return results
        
        except Exception as e:
            self.logger.error(f"Error in seasonal analysis: {e}")
            return {}
    
    def power_factor_analysis(self) -> Dict:
        """Analyze power factor."""
        try:
            self.logger.info("Analyzing power factor...")
            
            if 'power_factor' not in self.df.columns:
                if 'Apparent_Power_kVA' in self.df.columns:
                    self.df['power_factor'] = (
                        self.df['Active_Power_kW'] / self.df['Apparent_Power_kVA']
                    ).where(self.df['Apparent_Power_kVA'] != 0, 0)
                else:
                    return {}
            
            pf_stats = {
                'mean_pf': float(self.df['power_factor'].mean()),
                'median_pf': float(self.df['power_factor'].median()),
                'std_pf': float(self.df['power_factor'].std()),
                'min_pf': float(self.df['power_factor'].min()),
                'max_pf': float(self.df['power_factor'].max()),
                'poor_pf_records': int((self.df['power_factor'] < 0.85).sum()),
                'poor_pf_percentage': float((self.df['power_factor'] < 0.85).sum() / len(self.df) * 100)
            }
            
            return pf_stats
        
        except Exception as e:
            self.logger.error(f"Error in power factor analysis: {e}")
            return {}
    
    def load_profile(self) -> Dict:
        """Generate load profile by hour and day of week."""
        try:
            self.logger.info("Generating load profile...")
            
            if 'Timestamp' in self.df.columns and self.df['Timestamp'].dtype == 'object':
                self.df['Timestamp'] = pd.to_datetime(self.df['Timestamp'], errors='coerce')
            
            if 'hour' not in self.df.columns:
                self.df['hour'] = self.df['Timestamp'].dt.hour
            
            if 'day_of_week' not in self.df.columns:
                self.df['day_of_week'] = self.df['Timestamp'].dt.dayofweek
            
            load_profile = self.df.groupby(['day_of_week', 'hour'])['Active_Power_kW'].mean().reset_index()
            load_profile = load_profile.sort_values(['day_of_week', 'hour'])
            
            day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            load_profile['day_name'] = load_profile['day_of_week'].apply(lambda x: day_names[x % 7])
            
            results = {
                'load_profile': load_profile.to_dict('records'),
                'weekday_avg': float(self.df[self.df['day_of_week'] < 5]['Active_Power_kW'].mean()),
                'weekend_avg': float(self.df[self.df['day_of_week'] >= 5]['Active_Power_kW'].mean())
            }
            
            return results
        
        except Exception as e:
            self.logger.error(f"Error in load profile: {e}")
            return {}
    
    def run_all_analyses(self, data_file: str) -> Dict:
        """Run all analytics."""
        if not self.load_data(data_file):
            return {}
        
        all_results = {
            'timestamp': datetime.now().isoformat(),
            'data_file': data_file,
            'record_count': len(self.df),
            'peak_hours': self.identify_peak_hours(),
            'zone_analysis': self.zone_wise_analysis(),
            'top_consumers': self.top_consumers(),
            'anomalies': self.anomaly_detection(),
            'seasonal': self.seasonal_analysis(),
            'power_factor': self.power_factor_analysis(),
            'load_profile': self.load_profile()
        }
        
        return all_results
    
    def export_report(self, results: Dict, filename: str = "analytics_report.json"):
        """Export results to JSON."""
        try:
            import json
            
            output_path = self.output_dir / filename
            
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            self.logger.info(f"Report exported to: {output_path}")
            return str(output_path)
        
        except Exception as e:
            self.logger.error(f"Error exporting report: {e}")
            return None
    
    def export_summary_text(self, results: Dict, filename: str = "analytics_summary.txt"):
        """Export summary to text file."""
        try:
            output_path = self.output_dir / filename
            
            with open(output_path, 'w') as f:
                f.write("=" * 80 + "\n")
                f.write("SMART METER MDMS - ANALYTICS SUMMARY\n")
                f.write("=" * 80 + "\n\n")
                
                f.write(f"Report Generated: {results['timestamp']}\n")
                f.write(f"Records Analyzed: {results['record_count']:,}\n\n")
                
                # Peak Hours
                f.write("-" * 80 + "\n")
                f.write("PEAK HOURS ANALYSIS\n")
                f.write("-" * 80 + "\n")
                if results['peak_hours']:
                    ph = results['peak_hours']
                    f.write(f"Peak Hour: {ph.get('peak_hour', 'N/A')} (Avg: {ph.get('peak_avg_power', 0):.2f} kW)\n")
                    f.write(f"Off-Peak Hour: {ph.get('off_peak_hour', 'N/A')} (Avg: {ph.get('off_peak_avg_power', 0):.2f} kW)\n\n")
                
                # Zone Analysis
                f.write("-" * 80 + "\n")
                f.write("ZONE-WISE CONSUMPTION\n")
                f.write("-" * 80 + "\n")
                if results['zone_analysis']:
                    za = results['zone_analysis']
                    if 'highest_consumption_zone' in za:
                        hcz = za['highest_consumption_zone']
                        f.write(f"Highest: {hcz.get('zone', 'N/A')} ({hcz.get('avg_power', 0):.2f} kW)\n")
                    if 'lowest_consumption_zone' in za:
                        lcz = za['lowest_consumption_zone']
                        f.write(f"Lowest: {lcz.get('zone', 'N/A')} ({lcz.get('avg_power', 0):.2f} kW)\n\n")
                
                # Top Consumers
                f.write("-" * 80 + "\n")
                f.write("TOP 10 CONSUMING METERS\n")
                f.write("-" * 80 + "\n")
                if results['top_consumers'] and 'top_consumers' in results['top_consumers']:
                    for i, meter in enumerate(results['top_consumers']['top_consumers'][:10], 1):
                        f.write(f"{i:2d}. {meter.get('Meter_ID', 'N/A'):15s} - {meter.get('mean', 0):8.2f} kW (Zone: {meter.get('Zone', 'N/A')})\n")
                f.write("\n")
                
                # Anomalies
                f.write("-" * 80 + "\n")
                f.write("ANOMALY DETECTION\n")
                f.write("-" * 80 + "\n")
                if results['anomalies']:
                    anom = results['anomalies']
                    f.write(f"Total Anomalies: {anom.get('total_anomalies', 0):,}\n")
                    f.write(f"Anomaly Rate: {anom.get('anomaly_rate', 0):.2f}%\n")
                    f.write(f"Detection Method: {anom.get('detection_method', 'N/A')}\n")
                    bounds = anom.get('bounds', {})
                    f.write(f"Lower Bound: {bounds.get('lower_bound', 0):.2f} kW\n")
                    f.write(f"Upper Bound: {bounds.get('upper_bound', 0):.2f} kW\n\n")
                
                # Seasonal Trends
                f.write("-" * 80 + "\n")
                f.write("SEASONAL TRENDS\n")
                f.write("-" * 80 + "\n")
                if results['seasonal']:
                    seas = results['seasonal']
                    if 'highest_consumption_month' in seas:
                        hcm = seas['highest_consumption_month']
                        f.write(f"Highest: {hcm.get('month', 'N/A')} ({hcm.get('avg_power', 0):.2f} kW)\n")
                    if 'lowest_consumption_month' in seas:
                        lcm = seas['lowest_consumption_month']
                        f.write(f"Lowest: {lcm.get('month', 'N/A')} ({lcm.get('avg_power', 0):.2f} kW)\n\n")
                
                # Power Factor
                f.write("-" * 80 + "\n")
                f.write("POWER FACTOR ANALYSIS\n")
                f.write("-" * 80 + "\n")
                if results['power_factor']:
                    pf = results['power_factor']
                    f.write(f"Mean Power Factor: {pf.get('mean_pf', 0):.3f}\n")
                    f.write(f"Records with Poor PF (<0.85): {pf.get('poor_pf_records', 0):,} ({pf.get('poor_pf_percentage', 0):.2f}%)\n\n")
                
                f.write("=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")
            
            self.logger.info(f"Summary exported to: {output_path}")
            return str(output_path)
        
        except Exception as e:
            self.logger.error(f"Error exporting summary: {e}")
            return None


def main():
    """Example usage."""
    project_root = Path(__file__).parent.parent.parent
    
    # Find input file
    possible_files = [
        project_root / 'data' / 'processed' / 'processed_spark_output.csv',
        project_root / 'data' / 'output_processed.csv',
        project_root / 'data' / 'raw' / 'raw_smart_meter.csv'
    ]
    
    input_file = None
    for f in possible_files:
        if f.exists():
            input_file = str(f)
            break
    
    if not input_file:
        print("No input data file found")
        return
    
    engine = AnalyticsEngine(output_dir=str(project_root / 'outputs' / 'reports'))
    results = engine.run_all_analyses(input_file)
    
    if results:
        engine.export_report(results, "analytics_report.json")
        engine.export_summary_text(results, "analytics_summary.txt")


if __name__ == "__main__":
    main()
