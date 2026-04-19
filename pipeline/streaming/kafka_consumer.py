"""
Kafka Consumer - Consume real-time smart meter data from streaming topic
Processes streaming data and passes to PySpark for transformation.
"""

import pandas as pd
import json
import logging
from pathlib import Path
from typing import List, Dict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    from confluent_kafka import Consumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    logging.warning("confluent_kafka not installed")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger(__name__)


class StreamingDataConsumer:
    """
    Consumes smart meter data from Kafka stream and provides interface to PySpark.
    """
    
    def __init__(self, streaming_source: str = 'file', 
                 bootstrap_servers: str = 'localhost:9092',
                 topic: str = 'smart-meter-data'):
        """
        Initialize consumer.
        
        Args:
            streaming_source: 'kafka' or 'file'
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic
        """
        self.streaming_source = streaming_source
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.messages = []
        self.df = None
        
        logger.info(f"StreamingDataConsumer initialized with source={streaming_source}")
    
    def consume_from_file(self, file_path: str) -> pd.DataFrame:
        """
        Consume streaming data from file (simulated Kafka).
        
        Args:
            file_path: Path to streaming data file
            
        Returns:
            DataFrame with consumed data
        """
        try:
            logger.info(f"Reading streaming data from file: {file_path}")
            self.df = pd.read_csv(file_path)
            logger.info(f"Consumed {len(self.df)} records from streaming source")
            return self.df
        except Exception as e:
            logger.error(f"Error consuming from file: {e}")
            return pd.DataFrame()
    
    def consume_from_kafka(self, num_messages: int = 1000, 
                          timeout_ms: int = 1000) -> pd.DataFrame:
        """
        Consume streaming data from actual Kafka topic.
        
        Args:
            num_messages: Number of messages to consume
            timeout_ms: Timeout for consuming
            
        Returns:
            DataFrame with consumed data
        """
        if not KAFKA_AVAILABLE:
            logger.warning("Kafka not available, falling back to file-based simulation")
            return pd.DataFrame()
        
        try:
            from confluent_kafka import Consumer
            
            conf = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': 'smart-meter-spark',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            }
            
            consumer = Consumer(conf)
            consumer.subscribe([self.topic])
            
            logger.info(f"Consuming {num_messages} messages from Kafka topic '{self.topic}'")
            
            messages_consumed = 0
            while messages_consumed < num_messages:
                msg = consumer.poll(timeout_ms=timeout_ms)
                
                if msg is None:
                    break
                
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    break
                
                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    self.messages.append(data)
                    messages_consumed += 1
                    
                    if messages_consumed % 100 == 0:
                        logger.info(f"Consumed {messages_consumed} messages")
                
                except json.JSONDecodeError:
                    logger.warning("Failed to decode JSON message")
            
            consumer.close()
            
            if self.messages:
                self.df = pd.DataFrame(self.messages)
                logger.info(f"Successfully consumed {len(self.df)} records from Kafka")
            
            return self.df
        
        except Exception as e:
            logger.error(f"Error consuming from Kafka: {e}")
            return pd.DataFrame()
    
    def get_dataframe(self) -> pd.DataFrame:
        """Get consumed data as DataFrame."""
        return self.df if self.df is not None else pd.DataFrame()
    
    def save_local_copy(self, output_file: str):
        """Save consumed data to local file."""
        if self.df is not None and len(self.df) > 0:
            self.df.to_csv(output_file, index=False)
            logger.info(f"Saved {len(self.df)} records to {output_file}")


def main():
    """Example: Consume and save streaming data."""
    consumer = StreamingDataConsumer(streaming_source='file')
    
    # Try to load from streaming output
    streaming_file = Path(__file__).parent.parent.parent / 'data' / 'streaming_output.csv'
    if streaming_file.exists():
        df = consumer.consume_from_file(str(streaming_file))
        print(f"\nConsumed data shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        print(f"\nFirst records:\n{df.head()}")


if __name__ == "__main__":
    main()
