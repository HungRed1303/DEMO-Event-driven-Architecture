from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from abc import ABC, abstractmethod
from django.conf import settings

logger = logging.getLogger(__name__)

class BaseKafkaConsumer(ABC):
    """
    Base class cho tất cả Kafka consumers
    Cung cấp common functionality như error handling, retry, logging
    """
    
    def __init__(self, topic, group_id, process_name):
        """
        Initialize consumer
        
        Args:
            topic: Kafka topic to subscribe
            group_id: Consumer group ID
            process_name: Name of the process for logging
        """
        self.topic = topic
        self.group_id = group_id
        self.process_name = process_name
        self.consumer = None
        self._initialize_consumer()
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with error handling"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Manual commit for better control
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                max_poll_records=10,  # Process in batches
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info(f"[{self.process_name}] Consumer initialized successfully")
        except Exception as e:
            logger.error(f"[{self.process_name}] Failed to initialize consumer: {e}")
            raise
    
    @abstractmethod
    def process_message(self, message_data):
        """
        Abstract method to be implemented by subclasses
        
        Args:
            message_data: Deserialized message data
        
        Returns:
            bool: True if processing successful, False otherwise
        """
        pass
    
    def start(self):
        """Start consuming messages"""
        logger.info(f"[{self.process_name}] Starting to consume from topic: {self.topic}")
        
        try:
            for message in self.consumer:
                try:
                    logger.debug(
                        f"[{self.process_name}] Received message - "
                        f"Partition: {message.partition}, "
                        f"Offset: {message.offset}"
                    )
                    
                    # Process message
                    success = self.process_message(message.value)
                    
                    if success:
                        # Commit offset only if processing successful
                        self.consumer.commit()
                        logger.info(
                            f"[{self.process_name}] Successfully processed and committed - "
                            f"Offset: {message.offset}"
                        )
                    else:
                        logger.warning(
                            f"[{self.process_name}] Processing failed - "
                            f"Offset: {message.offset}"
                        )
                        # Don't commit - message will be reprocessed
                    
                except json.JSONDecodeError as e:
                    logger.error(f"[{self.process_name}] Invalid JSON: {e}")
                    # Commit to skip invalid message
                    self.consumer.commit()
                    
                except Exception as e:
                    logger.error(
                        f"[{self.process_name}] Error processing message: {e}",
                        exc_info=True
                    )
                    # Don't commit - will retry
                    
        except KeyboardInterrupt:
            logger.info(f"[{self.process_name}] Shutting down gracefully...")
        except Exception as e:
            logger.error(f"[{self.process_name}] Fatal error: {e}", exc_info=True)
        finally:
            self.close()
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info(f"[{self.process_name}] Consumer closed successfully")
            except Exception as e:
                logger.error(f"[{self.process_name}] Error closing consumer: {e}")