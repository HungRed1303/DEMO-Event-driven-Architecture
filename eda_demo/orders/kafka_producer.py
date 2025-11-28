from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
from django.conf import settings
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# Singleton producer instance
_producer = None

def get_producer():
    """Get or create Kafka producer instance"""
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Wait for all replicas
                retries=3,
                max_in_flight_requests_per_connection=1,  # Preserve order
                compression_type='gzip',
            )
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    return _producer

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def publish_order_event(order_data, topic=None):
    """
    Publish order event to Kafka with retry mechanism
    
    Args:
        order_data: Dictionary containing order information
        topic: Kafka topic name (default: settings.KAFKA_TOPIC_ORDERS)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if topic is None:
        topic = settings.KAFKA_TOPIC_ORDERS
    
    try:
        producer = get_producer()
        
        # Add metadata
        event_data = {
            **order_data,
            'event_type': 'order_created',
            'version': '1.0'
        }
        
        # Send with callback
        future = producer.send(topic, event_data)
        
        # Block for 'synchronous' send (up to 60 seconds)
        record_metadata = future.get(timeout=60)
        
        logger.info(
            f"Event published successfully - "
            f"Topic: {record_metadata.topic}, "
            f"Partition: {record_metadata.partition}, "
            f"Offset: {record_metadata.offset}, "
            f"Order ID: {order_data.get('id')}"
        )
        
        return True
        
    except KafkaError as e:
        logger.error(f"Kafka error while publishing event: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error while publishing event: {e}", exc_info=True)
        raise

def publish_to_dlq(order_data, error_message):
    """Publish failed message to Dead Letter Queue"""
    try:
        dlq_data = {
            **order_data,
            'error': error_message,
            'original_topic': settings.KAFKA_TOPIC_ORDERS
        }
        
        producer = get_producer()
        producer.send(settings.KAFKA_TOPIC_ORDERS_DLQ, dlq_data)
        producer.flush()
        
        logger.warning(f"Message sent to DLQ - Order ID: {order_data.get('id')}")
    except Exception as e:
        logger.error(f"Failed to send message to DLQ: {e}", exc_info=True)

def close_producer():
    """Close the producer connection"""
    global _producer
    if _producer is not None:
        try:
            _producer.close()
            logger.info("Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}")
        finally:
            _producer = None