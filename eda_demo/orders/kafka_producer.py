"""
Improved Kafka Producer with:
- Circuit Breaker pattern
- Detailed error handling
- Fallback mechanism
- Health monitoring
"""
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable
import json
import logging
from django.conf import settings
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)

# Singleton producer instance
_producer = None

# Circuit Breaker states
class CircuitState(Enum):
    CLOSED = "closed"      # Hoạt động bình thường
    OPEN = "open"          # Dừng gửi request, trả lỗi ngay
    HALF_OPEN = "half_open"  # Thử lại sau một thời gian

class CircuitBreaker:
    """
    Circuit Breaker pattern để tránh spam requests khi Kafka down
    
    States:
    - CLOSED: Bình thường, cho phép tất cả requests
    - OPEN: Kafka down, reject tất cả requests ngay lập tức
    - HALF_OPEN: Thử lại sau timeout, nếu OK → CLOSED, nếu fail → OPEN
    """
    
    def __init__(self, failure_threshold=5, timeout=60):
        """
        Args:
            failure_threshold: Số lần fail liên tiếp để mở circuit
            timeout: Thời gian (giây) trước khi thử lại
        """
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    def record_success(self):
        """Ghi nhận thành công → reset counter"""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        logger.info("[Circuit Breaker] ✅ Success recorded, circuit CLOSED")
    
    def record_failure(self):
        """Ghi nhận thất bại → tăng counter"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.error(
                f"[Circuit Breaker] 🔴 Circuit OPENED after {self.failure_count} failures. "
                f"Will retry after {self.timeout}s"
            )
        else:
            logger.warning(
                f"[Circuit Breaker] ⚠️  Failure {self.failure_count}/{self.failure_threshold}"
            )
    
    def can_attempt(self):
        """
        Kiểm tra xem có được phép gửi request không
        
        Returns:
            bool: True nếu được phép, False nếu circuit open
        """
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            # Kiểm tra timeout
            if datetime.now() - self.last_failure_time > timedelta(seconds=self.timeout):
                self.state = CircuitState.HALF_OPEN
                logger.info("[Circuit Breaker] 🟡 Circuit HALF_OPEN, trying again...")
                return True
            else:
                remaining = self.timeout - (datetime.now() - self.last_failure_time).seconds
                logger.warning(
                    f"[Circuit Breaker] 🔴 Circuit still OPEN, "
                    f"retry in {remaining}s"
                )
                return False
        
        # HALF_OPEN state
        return True

# Global circuit breaker instance
_circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=30)

def get_producer():
    """Get or create Kafka producer instance"""
    global _producer
    
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Đợi tất cả replicas confirm
                retries=3,   # Tự động retry 3 lần
                max_in_flight_requests_per_connection=1,  # Đảm bảo thứ tự
                compression_type='gzip',
                request_timeout_ms=10000,  # 10s timeout
                api_version=(2, 0, 0),
            )
            logger.info("✅ Kafka producer initialized successfully")
            _circuit_breaker.record_success()
            
        except NoBrokersAvailable as e:
            logger.error("❌ No Kafka brokers available!")
            _circuit_breaker.record_failure()
            raise
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka producer: {e}")
            _circuit_breaker.record_failure()
            raise
    
    return _producer

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True
)
def publish_order_event(order_data, topic=None):
    """
    Publish order event to Kafka with retry mechanism and circuit breaker
    
    Args:
        order_data: Dictionary chứa order information
        topic: Kafka topic name (default: settings.KAFKA_TOPIC_ORDERS)
    
    Returns:
        dict: {
            'success': bool,
            'metadata': record_metadata hoặc None,
            'error': error message hoặc None
        }
    """
    if topic is None:
        topic = settings.KAFKA_TOPIC_ORDERS
    
    # ========================
    # CIRCUIT BREAKER CHECK
    # ========================
    if not _circuit_breaker.can_attempt():
        error_msg = "Circuit breaker is OPEN. Kafka is currently unavailable."
        logger.error(f"🔴 {error_msg}")
        return {
            'success': False,
            'metadata': None,
            'error': error_msg,
            'circuit_state': _circuit_breaker.state.value
        }
    
    try:
        producer = get_producer()
        
        # Add metadata
        event_data = {
            **order_data,
            'event_type': 'order_created',
            'version': '1.0',
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(
            f"📤 [Producer] Sending event to Kafka - "
            f"Topic: {topic}, Order ID: {order_data.get('id')}"
        )
        
        # Send with callback (synchronous - block until confirmed)
        future = producer.send(topic, event_data)
        
        # Block for 'synchronous' send (up to 60 seconds)
        record_metadata = future.get(timeout=60)
        
        # ✅ SUCCESS
        _circuit_breaker.record_success()
        
        logger.info(
            f"✅ [Producer] Event published successfully - "
            f"Topic: {record_metadata.topic}, "
            f"Partition: {record_metadata.partition}, "
            f"Offset: {record_metadata.offset}, "
            f"Order ID: {order_data.get('id')}"
        )
        
        return {
            'success': True,
            'metadata': {
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'timestamp': record_metadata.timestamp
            },
            'error': None,
            'circuit_state': _circuit_breaker.state.value
        }
        
    except KafkaTimeoutError as e:
        # ❌ TIMEOUT ERROR
        _circuit_breaker.record_failure()
        error_msg = f"Kafka timeout: {str(e)}"
        logger.error(f"⏱️  [Producer] {error_msg}", exc_info=True)
        
        return {
            'success': False,
            'metadata': None,
            'error': error_msg,
            'error_type': 'timeout',
            'circuit_state': _circuit_breaker.state.value
        }
        
    except NoBrokersAvailable as e:
        # ❌ NO BROKERS AVAILABLE
        _circuit_breaker.record_failure()
        error_msg = "No Kafka brokers available"
        logger.error(f"🔴 [Producer] {error_msg}", exc_info=True)
        
        return {
            'success': False,
            'metadata': None,
            'error': error_msg,
            'error_type': 'no_brokers',
            'circuit_state': _circuit_breaker.state.value
        }
        
    except KafkaError as e:
        # ❌ GENERIC KAFKA ERROR
        _circuit_breaker.record_failure()
        error_msg = f"Kafka error: {str(e)}"
        logger.error(f"❌ [Producer] {error_msg}", exc_info=True)
        
        return {
            'success': False,
            'metadata': None,
            'error': error_msg,
            'error_type': 'kafka_error',
            'circuit_state': _circuit_breaker.state.value
        }
        
    except Exception as e:
        # ❌ UNEXPECTED ERROR
        _circuit_breaker.record_failure()
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"💥 [Producer] {error_msg}", exc_info=True)
        
        return {
            'success': False,
            'metadata': None,
            'error': error_msg,
            'error_type': 'unexpected',
            'circuit_state': _circuit_breaker.state.value
        }

def publish_to_dlq(order_data, error_message):
    """
    Publish failed message to Dead Letter Queue
    
    Args:
        order_data: Original order data
        error_message: Error description
    """
    try:
        dlq_data = {
            **order_data,
            'error': error_message,
            'failed_at': datetime.now().isoformat(),
            'original_topic': settings.KAFKA_TOPIC_ORDERS
        }
        
        producer = get_producer()
        producer.send(settings.KAFKA_TOPIC_ORDERS_DLQ, dlq_data)
        producer.flush()
        
        logger.warning(
            f"⚠️  [DLQ] Message sent to Dead Letter Queue - "
            f"Order ID: {order_data.get('id')}, Error: {error_message}"
        )
        
    except Exception as e:
        logger.error(f"💥 [DLQ] Failed to send message to DLQ: {e}", exc_info=True)

def get_circuit_breaker_status():
    """
    Get current circuit breaker status
    
    Returns:
        dict: Circuit breaker state information
    """
    return {
        'state': _circuit_breaker.state.value,
        'failure_count': _circuit_breaker.failure_count,
        'failure_threshold': _circuit_breaker.failure_threshold,
        'last_failure_time': _circuit_breaker.last_failure_time.isoformat() 
            if _circuit_breaker.last_failure_time else None,
        'timeout': _circuit_breaker.timeout
    }

def close_producer():
    """Close the producer connection"""
    global _producer
    if _producer is not None:
        try:
            _producer.close()
            logger.info("✅ Kafka producer closed successfully")
        except Exception as e:
            logger.error(f"❌ Error closing Kafka producer: {e}")
        finally:
            _producer = None