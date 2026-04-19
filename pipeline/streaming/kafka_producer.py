"""
Kafka Producer - Simulate real-time smart meter data streaming
Produces smart meter readings to Kafka topic for real-time processing.
"""

import pandas as pd
import numpy as np
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from confluent_kafka import Producer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("confluent_kafka not installed. Using file-based simulation.")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaProducerSimulator:
    """
    Simulates real-time smart meter data production.
    Can use actual Kafka or file-based simulation.
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092', 
                 topic: str = 'smart-meter-data',
                 use_simulation: bool = True):
        """
        Initialize streaming producer.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic name
            use_simulation: Use file-based simulation instead of Kafka
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.use_simulation = use_simulation or not KAFKA_AVAILABLE
        self.producer = None
        self.messages_produced = 0
        self.simulated_messages = []
        
        if not self.use_simulation and KAFKA_AVAILABLE:
            self._init_kafka_producer()
        
        logger.info(f"KafkaProductSimulator initialized (use_simulation={self.use_simulation})")
    
    def _init_kafka_producer(self):
        """Initialize Kafka producer."""
        try:
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'smart-meter-producer'
            }
            self.producer = Producer(conf)
            logger.info("Kafka Producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}. Switching to simulation mode.")
            self.use_simulation = True
    
    def produce_message(self, meter_data: Dict) -> bool:
        """
        Produce a single meter reading message.
        
        Args:
            meter_data: Dictionary with meter reading
            
        Returns:
            True if successful, False otherwise
        """
        try:
            message = json.dumps(meter_data)
            
            if self.use_simulation:
                self.simulated_messages.append(meter_data)
            else:
                self.producer.produce(
                    self.topic,
                    key=str(meter_data['Meter_ID']).encode('utf-8'),
                    value=message.encode('utf-8')
                )
                self.producer.flush()
            
            self.messages_produced += 1
            return True
        
        except Exception as e:
            logger.error(f"Error producing message: {e}")
            return False
    
    def stream_data_from_csv(self, csv_file: str, batch_size: int = 100,
                            delay_seconds: float = 0.1) -> int:
        """
        Stream data from CSV file in batches (simulating real-time ingestion).
        
        Args:
            csv_file: Path to CSV file
            batch_size: Number of records per batch
            delay_seconds: Delay between batches
            
        Returns:
            Total messages produced
        """
        logger.info(f"Starting to stream data from {csv_file}")
        logger.info(f"Batch size: {batch_size}, Delay: {delay_seconds}s")
        
        try:
            df = pd.read_csv(csv_file)
            logger.info(f"Loaded {len(df)} records from CSV")
            
            batch_count = 0
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                batch_count += 1
                
                for _, row in batch.iterrows():
                    meter_data = {
                        'Timestamp': str(row['Timestamp']),
                        'Meter_ID': str(row['Meter_ID']),
                        'Zone_ID': str(row['Zone_ID']),
                        'Voltage_V': float(row['Voltage_V']),
                        'Current_A': float(row['Current_A']),
                        'Active_Power_kW': float(row['Active_Power_kW']),
                        'Reactive_Power_kW': float(row['Reactive_Power_kW']),
                        'Apparent_Power_kVA': float(row['Apparent_Power_kVA']),
                        'Frequency_Hz': float(row['Frequency_Hz']),
                        'Sub_Meter_Kitchen': float(row['Sub_Meter_Kitchen']),
                        'Sub_Meter_HVAC': float(row['Sub_Meter_HVAC']),
                        'Outdoor_Temp_C': float(row['Outdoor_Temp_C']),
                        'ingestion_timestamp': datetime.now().isoformat()
                    }
                    
                    success = self.produce_message(meter_data)
                    if not success:
                        logger.warning(f"Failed to produce message for meter {meter_data['Meter_ID']}")
                
                logger.info(f"Batch {batch_count}: Produced {len(batch)} messages")
                
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
            
            logger.info(f"Streaming complete. Total messages produced: {self.messages_produced}")
            return self.messages_produced
        
        except Exception as e:
            logger.error(f"Error streaming data: {e}")
            return self.messages_produced
    
    def generate_synthetic_data(self, n_meters: int = 10, n_records: int = 1000,
                               delay_seconds: float = 0.05) -> int:
        """
        Generate synthetic real-time data and produce to stream.
        
        Args:
            n_meters: Number of meters to simulate
            n_records: Total records to generate
            delay_seconds: Delay between records
            
        Returns:
            Total messages produced
        """
        logger.info(f"Generating synthetic data: {n_meters} meters, {n_records} records")
        
        np.random.seed(42)
        base_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        
        for i in range(n_records):
            meter_id = f"MTR{np.random.randint(1, n_meters+1):05d}"
            zone_id = f"ZONE{np.random.randint(1, 6):01d}"
            
            # Realistic patterns
            hour = (i // (n_records // 24)) % 24
            base_power = 2 + 3 * np.sin(2 * np.pi * hour / 24)  # Diurnal pattern
            
            meter_data = {
                'Timestamp': (base_time + timedelta(hours=i/100)).isoformat(),
                'Meter_ID': meter_id,
                'Zone_ID': zone_id,
                'Voltage_V': np.random.normal(230, 5),
                'Current_A': max(0.1, np.abs(base_power + np.random.normal(0, 1))),
                'Active_Power_kW': max(0, base_power + np.random.normal(0, 0.5)),
                'Reactive_Power_kW': max(0, np.random.uniform(0, 1.5)),
                'Apparent_Power_kVA': max(0, base_power + np.random.normal(0, 0.5)),
                'Frequency_Hz': np.random.normal(50, 0.05),
                'Sub_Meter_Kitchen': max(0, np.random.uniform(0, 0.5)),
                'Sub_Meter_HVAC': max(0, base_power * np.random.uniform(0.3, 0.7)),
                'Outdoor_Temp_C': 25 + 10 * np.sin(2 * np.pi * hour / 24),
                'ingestion_timestamp': datetime.now().isoformat()
            }
            
            self.produce_message(meter_data)
            
            if (i + 1) % 100 == 0:
                logger.info(f"Generated {i+1}/{n_records} synthetic records")
            
            if delay_seconds > 0:
                time.sleep(delay_seconds)
        
        logger.info(f"Synthetic data generation complete. Total: {self.messages_produced} messages")
        return self.messages_produced
    
    def get_simulated_data_batch(self, batch_size: int = 100) -> List[Dict]:
        """Get batch of simulated messages (for file-based mode)."""
        if len(self.simulated_messages) >= batch_size:
            batch = self.simulated_messages[:batch_size]
            self.simulated_messages = self.simulated_messages[batch_size:]
            return batch
        return self.simulated_messages
    
    def save_simulated_messages(self, output_file: str):
        """Save all simulated messages to CSV for consumption."""
        if self.simulated_messages:
            df = pd.DataFrame(self.simulated_messages)
            df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(self.simulated_messages)} messages to {output_file}")


class KafkaConsumerSimulator:
    """
    Simulates consuming smart meter data from Kafka stream.
    """
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'smart-meter-data',
                 group_id: str = 'smart-meter-group',
                 use_simulation: bool = True):
        """Initialize streaming consumer."""
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.use_simulation = use_simulation or not KAFKA_AVAILABLE
        self.consumer = None
        self.messages_consumed = 0
        self.messages = []
        
        if not self.use_simulation and KAFKA_AVAILABLE:
            self._init_kafka_consumer()
        
        logger.info(f"KafkaConsumerSimulator initialized (use_simulation={self.use_simulation})")
    
    def _init_kafka_consumer(self):
        """Initialize Kafka consumer."""
        try:
            from confluent_kafka import Consumer
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            }
            self.consumer = Consumer(conf)
            self.consumer.subscribe([self.topic])
            logger.info("Kafka Consumer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}. Switching to simulation mode.")
            self.use_simulation = True
    
    def consume_messages(self, timeout_ms: int = 1000, max_messages: int = None) -> List[Dict]:
        """Consume messages from stream."""
        logger.info(f"Consuming messages from {self.topic}...")
        
        try:
            while True:
                if max_messages and self.messages_consumed >= max_messages:
                    break
                
                msg = self.consumer.poll(timeout_ms=timeout_ms)
                
                if msg is None:
                    break
                
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    break
                
                try:
                    message_data = json.loads(msg.value().decode('utf-8'))
                    self.messages.append(message_data)
                    self.messages_consumed += 1
                    
                    if self.messages_consumed % 100 == 0:
                        logger.info(f"Consumed {self.messages_consumed} messages")
                
                except json.JSONDecodeError:
                    logger.warning("Failed to decode JSON message")
            
            logger.info(f"Consumption complete. Total messages: {self.messages_consumed}")
            return self.messages
        
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
            return self.messages
    
    def get_dataframe(self) -> pd.DataFrame:
        """Convert consumed messages to DataFrame."""
        if self.messages:
            return pd.DataFrame(self.messages)
        return pd.DataFrame()


def main():
    """Example: Stream data from CSV file."""
    producer = KafkaProducerSimulator(use_simulation=True)
    
    # Stream real data if available
    csv_file = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'raw_smart_meter.csv'
    if csv_file.exists():
        producer.stream_data_from_csv(str(csv_file), batch_size=50, delay_seconds=0.01)
        # Save for consumption
        producer.save_simulated_messages(
            str(Path(__file__).parent.parent.parent / 'data' / 'streaming_output.csv')
        )
    else:
        # Generate synthetic data
        producer.generate_synthetic_data(n_meters=10, n_records=500, delay_seconds=0.01)
        producer.save_simulated_messages(
            str(Path(__file__).parent.parent.parent / 'data' / 'streaming_output.csv')
        )


if __name__ == "__main__":
    main()
