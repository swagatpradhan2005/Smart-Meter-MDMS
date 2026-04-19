"""
Kafka Streaming Simulation Module
Simulates Kafka producer-consumer for streaming data.
Supports local mode simulation with CSV chunking.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Optional, List, Dict
import logging
import time
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)


class KafkaProducerSimulator:
    """Simulates Kafka producer - reads CSV and produces records."""
    
    def __init__(self, topic: str = "smart-meter-events", batch_size: int = 100):
        """
        Initialize producer.
        
        Args:
            topic: Topic name
            batch_size: Records per batch
        """
        self.topic = topic
        self.batch_size = batch_size
        self.messages_sent = 0
        logger.info(f"KafkaProducerSimulator initialized: topic={topic}")
    
    def produce_from_csv(self, csv_path: str) -> int:
        """
        Produce messages from CSV file.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            Total messages produced
        """
        try:
            df = pd.read_csv(csv_path)
            self.messages_sent = len(df)
            logger.info(f"Produced {self.messages_sent} messages from {csv_path} to topic {self.topic}")
            return self.messages_sent
        except Exception as e:
            logger.error(f"Produce failed: {e}")
            return 0


class KafkaConsumerSimulator:
    """Simulates Kafka consumer - consumes records and stores data."""
    
    def __init__(self, topic: str = "smart-meter-events", output_path: Optional[str] = None):
        """
        Initialize consumer.
        
        Args:
            topic: Topic name to consume from
            output_path: Path to save consumed data
        """
        self.topic = topic
        self.output_path = Path(output_path or f"data/stream/{topic}.csv")
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.messages_consumed = 0
        logger.info(f"KafkaConsumerSimulator initialized: topic={topic}, output={self.output_path}")
    
    def consume_from_csv(self, csv_path: str, sample_fraction: float = 1.0) -> pd.DataFrame:
        """
        Consume messages from CSV (simulation).
        
        Args:
            csv_path: Source CSV file
            sample_fraction: Fraction of records to consume
            
        Returns:
            Consumed DataFrame
        """
        try:
            df = pd.read_csv(csv_path)
            
            # Sample if needed
            if sample_fraction < 1.0:
                df = df.sample(frac=sample_fraction, random_state=42)
            
            self.messages_consumed = len(df)
            
            # Save to output
            df.to_csv(self.output_path, index=False)
            logger.info(f"Consumed {self.messages_consumed} messages, saved to {self.output_path}")
            
            return df
        except Exception as e:
            logger.error(f"Consume failed: {e}")
            return pd.DataFrame()


def simulate_kafka_streaming(input_csv: str, output_dir: str = "data/stream") -> Dict[str, int]:
    """
    Full Kafka simulation: produce + consume.
    
    Args:
        input_csv: Source CSV path
        output_dir: Output directory
        
    Returns:
        Statistics dictionary
    """
    logger.info("Starting Kafka streaming simulation...")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Produce
    producer = KafkaProducerSimulator(topic="smart-meter-events")
    produced = producer.produce_from_csv(input_csv)
    
    # Consume
    output_path = Path(output_dir) / "consumed_stream.csv"
    consumer = KafkaConsumerSimulator(output_path=str(output_path))
    df = consumer.consume_from_csv(input_csv)
    
    stats = {
        'topic': 'smart-meter-events',
        'messages_produced': produced,
        'messages_consumed': len(df),
        'output_file': str(output_path)
    }
    
    logger.info(f"Kafka streaming simulation completed: {stats}")
    return stats


def main():
    """Example usage."""
    logger.info("Kafka streaming simulation module loaded")


if __name__ == "__main__":
    main()
