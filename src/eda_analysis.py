"""
Exploratory Data Analysis (EDA) for Smart Meter Data
Generates comprehensive visualizations including:
- Temporal trends (hourly, daily, monthly)
- Zone-wise comparisons
- Peak analysis
- Distributions
- Anomaly patterns
- Top consumers
"""

import logging
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


class SmartMeterEDA:
    """Exploratory Data Analysis for Smart Meter MDMS."""
    
    def __init__(self, data_input, output_dir: str = "outputs/plots"):
        """
        Initialize EDA.
        
        Args:
            data_input: DataFrame or path to processed data CSV
            output_dir: Directory to save plots
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Accept both DataFrame and file path
        if isinstance(data_input, pd.DataFrame):
            self.df = data_input
            self.data_file = "DataFrame"
        else:
            self.data_file = data_input
            self.df = None
        
        self.logger = logger
    
    def load_data(self) -> bool:
        """Load and validate data."""
        try:
            if self.df is not None:
                # Already have a DataFrame
                self.logger.info(f"Using provided DataFrame with {len(self.df)} records")
                return True
            
            self.logger.info(f"Loading data from {self.data_file}...")
            self.df = pd.read_csv(self.data_file)
            self.logger.info(f"Loaded {len(self.df)} records, {len(self.df.columns)} columns")
            
            # Ensure required columns
            required = ['Active_Power_kW', 'Reactive_Power_kVAR', 'Meter_ID', 'Zone']
            missing = [c for c in required if c not in self.df.columns]
            
            if missing:
                self.logger.warning(f"Missing columns: {missing}")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            return False
    
    def plot_hourly_consumption(self):
        """Plot hourly consumption trend."""
        try:
            self.logger.info("Generating hourly consumption trend...")
            
            if 'hour' not in self.df.columns:
                self.df['hour'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.hour
            
            hourly_avg = self.df.groupby('hour')['Active_Power_kW'].agg(['mean', 'std']).reset_index()
            
            fig, ax = plt.subplots(figsize=(12, 6))
            ax.plot(hourly_avg['hour'], hourly_avg['mean'], marker='o', linewidth=2, markersize=8, label='Mean')
            ax.fill_between(hourly_avg['hour'], 
                            hourly_avg['mean'] - hourly_avg['std'],
                            hourly_avg['mean'] + hourly_avg['std'],
                            alpha=0.3, label='±1 Std')
            
            ax.set_xlabel('Hour of Day', fontsize=12)
            ax.set_ylabel('Active Power (kW)', fontsize=12)
            ax.set_title('Hourly Consumption Pattern', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3)
            ax.legend()
            
            output_path = self.output_dir / '01_hourly_consumption.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in hourly plot: {e}")
    
    def plot_daily_consumption(self):
        """Plot daily consumption trend."""
        try:
            self.logger.info("Generating daily consumption trend...")
            
            if 'day_of_month' not in self.df.columns:
                self.df['day_of_month'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.day
            
            daily_avg = self.df.groupby('day_of_month')['Active_Power_kW'].agg(['mean', 'count']).reset_index()
            daily_avg = daily_avg[daily_avg['count'] > 0]  # Filter empty days
            
            fig, ax = plt.subplots(figsize=(14, 6))
            bars = ax.bar(daily_avg['day_of_month'], daily_avg['mean'], color='steelblue', alpha=0.7)
            
            # Color top day differently
            top_day_idx = daily_avg['mean'].idxmax()
            bars[top_day_idx].set_color('coral')
            
            ax.set_xlabel('Day of Month', fontsize=12)
            ax.set_ylabel('Average Active Power (kW)', fontsize=12)
            ax.set_title('Daily Consumption Pattern', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='y')
            
            output_path = self.output_dir / '02_daily_consumption.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in daily plot: {e}")
    
    def plot_monthly_consumption(self):
        """Plot monthly consumption trend."""
        try:
            self.logger.info("Generating monthly consumption trend...")
            
            if 'month' not in self.df.columns:
                self.df['month'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.month
            
            monthly_avg = self.df.groupby('month')['Active_Power_kW'].mean().reset_index()
            month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            monthly_avg['month_name'] = monthly_avg['month'].apply(lambda x: month_names[x-1] if 1 <= x <= 12 else '')
            
            fig, ax = plt.subplots(figsize=(12, 6))
            colors = plt.cm.RdYlGn(np.linspace(0.3, 0.7, len(monthly_avg)))
            bars = ax.bar(monthly_avg['month_name'], monthly_avg['Active_Power_kW'], color=colors)
            
            ax.set_xlabel('Month', fontsize=12)
            ax.set_ylabel('Average Active Power (kW)', fontsize=12)
            ax.set_title('Monthly Consumption Trend', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}', ha='center', va='bottom')
            
            output_path = self.output_dir / '03_monthly_consumption.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in monthly plot: {e}")
    
    def plot_zone_comparison(self):
        """Plot zone-wise consumption comparison."""
        try:
            self.logger.info("Generating zone-wise consumption comparison...")
            
            if 'Zone' not in self.df.columns:
                self.logger.warning("Zone column not found")
                return
            
            zone_avg = self.df.groupby('Zone')['Active_Power_kW'].agg(['mean', 'std']).reset_index()
            zone_avg = zone_avg.sort_values('mean', ascending=False)
            
            fig, ax = plt.subplots(figsize=(12, 6))
            x_pos = np.arange(len(zone_avg))
            bars = ax.barh(x_pos, zone_avg['mean'], xerr=zone_avg['std'], 
                           capsize=5, color='teal', alpha=0.7, error_kw={'elinewidth': 2})
            
            ax.set_yticks(x_pos)
            ax.set_yticklabels(zone_avg['Zone'])
            ax.set_xlabel('Average Active Power (kW)', fontsize=12)
            ax.set_title('Zone-wise Consumption Comparison', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='x')
            
            output_path = self.output_dir / '04_zone_comparison.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in zone comparison: {e}")
    
    def plot_peak_analysis(self):
        """Plot peak vs non-peak usage comparison."""
        try:
            self.logger.info("Generating peak vs non-peak analysis...")
            
            if 'peak_hour_flag' not in self.df.columns:
                if 'hour' not in self.df.columns:
                    self.df['hour'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.hour
                self.df['peak_hour_flag'] = self.df['hour'].between(10, 22).astype(int)
            
            peak_data = self.df.groupby('peak_hour_flag')['Active_Power_kW'].agg(['mean', 'std', 'count']).reset_index()
            peak_data['period'] = peak_data['peak_hour_flag'].map({1: 'Peak Hours\n(10 AM - 10 PM)', 0: 'Off-Peak'})
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            # Bar chart
            colors = ['coral', 'lightblue']
            bars = ax1.bar(peak_data['period'], peak_data['mean'], color=colors, alpha=0.7, width=0.6)
            ax1.set_ylabel('Average Active Power (kW)', fontsize=11)
            ax1.set_title('Peak vs Non-Peak Comparison', fontsize=13, fontweight='bold')
            ax1.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height,
                        f'{height:.1f}', ha='center', va='bottom', fontweight='bold')
            
            # Box plot
            peak_labels = ['On-Peak', 'Off-Peak']
            peak_data_list = [self.df[self.df['peak_hour_flag'] == 1]['Active_Power_kW'],
                             self.df[self.df['peak_hour_flag'] == 0]['Active_Power_kW']]
            
            bp = ax2.boxplot(peak_data_list, labels=peak_labels, patch_artist=True)
            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
            
            ax2.set_ylabel('Active Power (kW)', fontsize=11)
            ax2.set_title('Power Distribution: Peak vs Non-Peak', fontsize=13, fontweight='bold')
            ax2.grid(True, alpha=0.3, axis='y')
            
            output_path = self.output_dir / '05_peak_analysis.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in peak analysis: {e}")
    
    def plot_power_distribution(self):
        """Plot power consumption distribution."""
        try:
            self.logger.info("Generating power distribution histogram...")
            
            fig, ax = plt.subplots(figsize=(12, 6))
            
            ax.hist(self.df['Active_Power_kW'], bins=50, color='purple', alpha=0.7, edgecolor='black')
            ax.axvline(self.df['Active_Power_kW'].mean(), color='red', linestyle='--', linewidth=2, label=f'Mean: {self.df["Active_Power_kW"].mean():.2f}')
            ax.axvline(self.df['Active_Power_kW'].median(), color='green', linestyle='--', linewidth=2, label=f'Median: {self.df["Active_Power_kW"].median():.2f}')
            
            ax.set_xlabel('Active Power (kW)', fontsize=12)
            ax.set_ylabel('Frequency', fontsize=12)
            ax.set_title('Distribution of Power Consumption', fontsize=14, fontweight='bold')
            ax.legend()
            ax.grid(True, alpha=0.3, axis='y')
            
            output_path = self.output_dir / '06_power_distribution.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in distribution plot: {e}")
    
    def plot_anomaly_analysis(self):
        """Plot anomaly distribution."""
        try:
            self.logger.info("Generating anomaly analysis...")
            
            if 'is_anomaly' not in self.df.columns:
                Q1 = self.df['Active_Power_kW'].quantile(0.25)
                Q3 = self.df['Active_Power_kW'].quantile(0.75)
                IQR = Q3 - Q1
                self.df['is_anomaly'] = ((self.df['Active_Power_kW'] < Q1 - 1.5*IQR) | 
                                        (self.df['Active_Power_kW'] > Q3 + 1.5*IQR)).astype(int)
            
            anomaly_count = self.df['is_anomaly'].sum()
            anomaly_pct = (anomaly_count / len(self.df)) * 100
            
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            
            # Pie chart
            sizes = [len(self.df) - anomaly_count, anomaly_count]
            colors_pie = ['lightgreen', 'salmon']
            axes[0, 0].pie(sizes, labels=['Normal', 'Anomaly'], autopct='%1.1f%%', colors=colors_pie, startangle=90)
            axes[0, 0].set_title('Anomaly Distribution', fontweight='bold')
            
            # Time series with anomalies highlighted
            if len(self.df) <= 1000:  # Only plot if manageable size
                axes[0, 1].scatter(range(len(self.df)), self.df['Active_Power_kW'], 
                                 c=self.df['is_anomaly'], cmap='RdYlGn_r', s=10, alpha=0.6)
                axes[0, 1].set_xlabel('Record Index')
                axes[0, 1].set_ylabel('Active Power (kW)')
                axes[0, 1].set_title('Anomalies in Time Series', fontweight='bold')
                axes[0, 1].grid(True, alpha=0.3)
            
            # Zone-wise anomalies
            if 'Zone' in self.df.columns:
                zone_anomalies = self.df.groupby('Zone')['is_anomaly'].sum().reset_index()
                zone_anomalies = zone_anomalies.sort_values('is_anomaly', ascending=False)
                axes[1, 0].barh(zone_anomalies['Zone'], zone_anomalies['is_anomaly'], color='coral', alpha=0.7)
                axes[1, 0].set_xlabel('Number of Anomalies')
                axes[1, 0].set_title('Anomalies by Zone', fontweight='bold')
                axes[1, 0].grid(True, alpha=0.3, axis='x')
            
            # Statistics
            axes[1, 1].axis('off')
            stats_text = f"""
            ANOMALY STATISTICS
            ──────────────────
            Total Anomalies: {anomaly_count:,}
            Anomaly Rate: {anomaly_pct:.2f}%
            Normal Records: {len(self.df) - anomaly_count:,}
            
            Detection Method: IQR (1.5×)
            Q1: {Q1:.2f} kW
            Q3: {Q3:.2f} kW
            IQR: {IQR:.2f} kW
            """
            axes[1, 1].text(0.1, 0.5, stats_text, fontsize=11, family='monospace',
                          verticalalignment='center', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            output_path = self.output_dir / '07_anomaly_analysis.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in anomaly analysis: {e}")
    
    def plot_top_consumers(self):
        """Plot top 10 highest consumption meters."""
        try:
            self.logger.info("Generating top consumers analysis...")
            
            if 'Meter_ID' not in self.df.columns:
                self.logger.warning("Meter_ID column not found")
                return
            
            meter_avg = self.df.groupby('Meter_ID')['Active_Power_kW'].mean().reset_index()
            meter_avg = meter_avg.sort_values('Active_Power_kW', ascending=False).head(10)
            
            fig, ax = plt.subplots(figsize=(12, 6))
            
            colors_gradient = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(meter_avg)))
            bars = ax.barh(range(len(meter_avg)), meter_avg['Active_Power_kW'], color=colors_gradient)
            
            ax.set_yticks(range(len(meter_avg)))
            ax.set_yticklabels(meter_avg['Meter_ID'])
            ax.set_xlabel('Average Active Power (kW)', fontsize=12)
            ax.set_title('Top 10 Highest Consumption Meters', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='x')
            ax.invert_yaxis()
            
            # Add value labels
            for i, bar in enumerate(bars):
                width = bar.get_width()
                ax.text(width, bar.get_y() + bar.get_height()/2.,
                       f' {width:.2f}', ha='left', va='center', fontweight='bold')
            
            output_path = self.output_dir / '08_top_consumers.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in top consumers: {e}")
    
    def plot_reactive_power(self):
        """Plot reactive power analysis."""
        try:
            self.logger.info("Generating reactive power analysis...")
            
            if 'Reactive_Power_kVAR' not in self.df.columns:
                self.logger.warning("Reactive_Power_kVAR column not found")
                return
            
            if 'hour' not in self.df.columns:
                self.df['hour'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.hour
            
            hourly_reactive = self.df.groupby('hour')['Reactive_Power_kVAR'].mean().reset_index()
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            # Time series
            ax1.plot(hourly_reactive['hour'], hourly_reactive['Reactive_Power_kVAR'], 
                    marker='o', linewidth=2, markersize=8, color='darkblue')
            ax1.fill_between(hourly_reactive['hour'], hourly_reactive['Reactive_Power_kVAR'], 
                            alpha=0.3, color='lightblue')
            ax1.set_xlabel('Hour of Day', fontsize=11)
            ax1.set_ylabel('Reactive Power (kVAR)', fontsize=11)
            ax1.set_title('Hourly Reactive Power Pattern', fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.3)
            
            # Scatter: Active vs Reactive
            ax2.scatter(self.df['Active_Power_kW'], self.df['Reactive_Power_kVAR'], 
                       alpha=0.3, s=20, color='darkgreen')
            ax2.set_xlabel('Active Power (kW)', fontsize=11)
            ax2.set_ylabel('Reactive Power (kVAR)', fontsize=11)
            ax2.set_title('Active vs Reactive Power Relationship', fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.3)
            
            output_path = self.output_dir / '09_reactive_power.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in reactive power analysis: {e}")
    
    def plot_voltage_analysis(self):
        """Plot voltage analysis."""
        try:
            self.logger.info("Generating voltage analysis...")
            
            if 'Voltage_V' not in self.df.columns:
                self.logger.warning("Voltage_V column not found")
                return
            
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            
            # Voltage distribution
            axes[0, 0].hist(self.df['Voltage_V'], bins=50, color='purple', alpha=0.7, edgecolor='black')
            axes[0, 0].axvline(self.df['Voltage_V'].mean(), color='red', linestyle='--', linewidth=2)
            axes[0, 0].set_xlabel('Voltage (V)')
            axes[0, 0].set_ylabel('Frequency')
            axes[0, 0].set_title('Voltage Distribution', fontweight='bold')
            axes[0, 0].grid(True, alpha=0.3, axis='y')
            
            # Voltage vs Power
            axes[0, 1].scatter(self.df['Voltage_V'], self.df['Active_Power_kW'], alpha=0.3, s=15)
            axes[0, 1].set_xlabel('Voltage (V)')
            axes[0, 1].set_ylabel('Active Power (kW)')
            axes[0, 1].set_title('Voltage vs Active Power', fontweight='bold')
            axes[0, 1].grid(True, alpha=0.3)
            
            # Hourly voltage
            if 'hour' not in self.df.columns:
                self.df['hour'] = pd.to_datetime(self.df['Timestamp'], errors='coerce').dt.hour
            
            hourly_voltage = self.df.groupby('hour')['Voltage_V'].mean().reset_index()
            axes[1, 0].plot(hourly_voltage['hour'], hourly_voltage['Voltage_V'], marker='o', color='darkblue')
            axes[1, 0].set_xlabel('Hour of Day')
            axes[1, 0].set_ylabel('Average Voltage (V)')
            axes[1, 0].set_title('Hourly Voltage Pattern', fontweight='bold')
            axes[1, 0].grid(True, alpha=0.3)
            
            # Voltage statistics
            axes[1, 1].axis('off')
            voltage_stats = f"""
            VOLTAGE STATISTICS
            ──────────────────
            Mean: {self.df['Voltage_V'].mean():.2f} V
            Std Dev: {self.df['Voltage_V'].std():.2f} V
            Min: {self.df['Voltage_V'].min():.2f} V
            Max: {self.df['Voltage_V'].max():.2f} V
            Median: {self.df['Voltage_V'].median():.2f} V
            """
            axes[1, 1].text(0.1, 0.5, voltage_stats, fontsize=11, family='monospace',
                          verticalalignment='center', bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.7))
            
            output_path = self.output_dir / '10_voltage_analysis.png'
            plt.tight_layout()
            plt.savefig(output_path, dpi=300)
            plt.close()
            
            self.logger.info(f"Saved: {output_path}")
        
        except Exception as e:
            self.logger.error(f"Error in voltage analysis: {e}")
    
    def generate_all_plots(self):
        """Generate all visualizations."""
        self.logger.info("=" * 80)
        self.logger.info("SMART METER EDA ANALYSIS START")
        self.logger.info("=" * 80)
        
        if not self.load_data():
            return False
        
        # Generate all plots
        self.plot_hourly_consumption()      # 01
        self.plot_daily_consumption()       # 02
        self.plot_monthly_consumption()     # 03
        self.plot_zone_comparison()         # 04
        self.plot_peak_analysis()           # 05
        self.plot_power_distribution()      # 06
        self.plot_anomaly_analysis()        # 07
        self.plot_top_consumers()           # 08
        self.plot_reactive_power()          # 09
        self.plot_voltage_analysis()        # 10
        
        self.logger.info("=" * 80)
        self.logger.info(f"ALL VISUALIZATIONS SAVED TO: {self.output_dir}")
        self.logger.info("=" * 80)
        
        return True


def main():
    """Example usage."""
    # Detect input file
    project_root = Path(__file__).parent.parent.parent
    
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
    
    output_dir = str(project_root / 'outputs' / 'plots')
    
    eda = SmartMeterEDA(input_file, output_dir)
    eda.generate_all_plots()


if __name__ == "__main__":
    main()
