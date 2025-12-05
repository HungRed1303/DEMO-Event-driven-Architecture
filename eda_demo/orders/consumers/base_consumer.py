"""
Improved Kafka Consumer with:
- Idempotency handling (tránh xử lý trùng)
- Automatic retry with exponential backoff
- Dead Letter Queue for failed messages
- Detailed logging
"""
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import logging
from abc import ABC, abstractmethod
from django.conf import settings
from django.core.cache import cache
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class BaseKafkaConsumer(ABC):
    """
    Base class cho tất cả Kafka consumers
    Cung cấp:
    - Error handling & retry
    - Idempotency checking
    - Dead Letter Queue
    - Detailed logging
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
        self.processed_messages = set()  # In-memory cache for current session
        self._initialize_consumer()
    
    def _initialize_consumer(self):
        """Initialize Kafka consumer with error handling"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                group_id=self.group_id,
                auto_offset_reset='earliest',  # Bắt đầu từ message đầu tiên chưa xử lý
                enable_auto_commit=False,      # Manual commit để control tốt hơn
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                max_poll_records=10,           # Xử lý theo batch
                session_timeout_ms=30000,      # 30s timeout
                heartbeat_interval_ms=10000,   # 10s heartbeat
                max_poll_interval_ms=300000,   # 5 minutes max processing time
            )
            logger.info(f"✅ [{self.process_name}] Consumer initialized successfully")
        except Exception as e:
            logger.error(f"❌ [{self.process_name}] Failed to initialize consumer: {e}")
            raise
    
    def _is_already_processed(self, message_data):
        """
        Kiểm tra xem message đã được xử lý chưa (Idempotency check)
        
        Sử dụng:
        1. In-memory set (nhanh nhưng mất khi restart)
        2. Django cache (Redis/Memcached - persist across restarts)
        
        Args:
            message_data: Message data với idempotency_key
        
        Returns:
            bool: True nếu đã xử lý, False nếu chưa
        """
        # Lấy idempotency key từ message
        idempotency_key = message_data.get('idempotency_key')
        order_id = message_data.get('id')
        
        # Nếu không có idempotency_key, dùng order_id + consumer group
        if not idempotency_key:
            idempotency_key = f"{self.group_id}:order:{order_id}"
        
        # 1. Check in-memory cache (fastest)
        if idempotency_key in self.processed_messages:
            logger.warning(
                f"⚠️  [{self.process_name}] DUPLICATE detected (in-memory) - "
                f"Order ID: {order_id}, Key: {idempotency_key}"
            )
            return True
        
        # 2. Check Redis cache (persistent)
        cache_key = f"processed:{self.group_id}:{idempotency_key}"
        if cache.get(cache_key):
            logger.warning(
                f"⚠️  [{self.process_name}] DUPLICATE detected (cache) - "
                f"Order ID: {order_id}, Key: {idempotency_key}"
            )
            # Add to in-memory cache for faster subsequent checks
            self.processed_messages.add(idempotency_key)
            return True
        
        return False
    
    def _mark_as_processed(self, message_data):
        """
        Đánh dấu message đã được xử lý
        
        Args:
            message_data: Message data
        """
        idempotency_key = message_data.get('idempotency_key')
        order_id = message_data.get('id')
        
        if not idempotency_key:
            idempotency_key = f"{self.group_id}:order:{order_id}"
        
        # 1. Add to in-memory set
        self.processed_messages.add(idempotency_key)
        
        # 2. Add to Redis cache (expire after 24 hours)
        cache_key = f"processed:{self.group_id}:{idempotency_key}"
        cache.set(cache_key, True, timeout=86400)  # 24 hours
        
        logger.debug(
            f"✅ [{self.process_name}] Marked as processed - "
            f"Order ID: {order_id}, Key: {idempotency_key}"
        )
    
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
    
    def _process_with_retry(self, message_data, max_retries=3):
        """
        Xử lý message với retry mechanism
        
        Args:
            message_data: Message data
            max_retries: Số lần retry tối đa
        
        Returns:
            bool: True if successful after retries
        """
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"🔄 [{self.process_name}] Processing attempt {attempt}/{max_retries} - "
                    f"Order ID: {message_data.get('id')}"
                )
                
                success = self.process_message(message_data)
                
                if success:
                    return True
                else:
                    logger.warning(
                        f"⚠️  [{self.process_name}] Processing returned False - "
                        f"Attempt {attempt}/{max_retries}"
                    )
            
            except Exception as e:
                logger.error(
                    f"❌ [{self.process_name}] Error on attempt {attempt}/{max_retries}: {e}",
                    exc_info=True
                )
            
            # Exponential backoff giữa các retry
            if attempt < max_retries:
                backoff = 2 ** attempt  # 2s, 4s, 8s
                logger.info(f"⏳ [{self.process_name}] Waiting {backoff}s before retry...")
                time.sleep(backoff)
        
        # All retries failed
        return False
    
    def _send_to_dlq(self, message_data, error_message):
        """
        Gửi message failed đến Dead Letter Queue
        
        Args:
            message_data: Original message data
            error_message: Error description
        """
        try:
            from .kafka_producer import get_producer
            
            dlq_data = {
                **message_data,
                'error': error_message,
                'failed_at': datetime.now().isoformat(),
                'consumer_group': self.group_id,
                'process_name': self.process_name
            }
            
            producer = get_producer()
            producer.send(settings.KAFKA_TOPIC_ORDERS_DLQ, dlq_data)
            producer.flush()
            
            logger.warning(
                f"⚠️  [{self.process_name}] Message sent to DLQ - "
                f"Order ID: {message_data.get('id')}"
            )
            
        except Exception as e:
            logger.error(
                f"💥 [{self.process_name}] Failed to send to DLQ: {e}",
                exc_info=True
            )
    
    def start(self):
        """Start consuming messages"""
        logger.info(
            f"🚀 [{self.process_name}] Starting to consume from topic: {self.topic}"
        )
        
        message_count = 0
        success_count = 0
        duplicate_count = 0
        failed_count = 0
        
        try:
            for message in self.consumer:
                message_count += 1
                
                try:
                    logger.info(
                        f"📨 [{self.process_name}] Received message #{message_count} - "
                        f"Partition: {message.partition}, "
                        f"Offset: {message.offset}, "
                        f"Order ID: {message.value.get('id')}"
                    )
                    
                    # ========================
                    # IDEMPOTENCY CHECK
                    # ========================
                    if self._is_already_processed(message.value):
                        duplicate_count += 1
                        # Skip nhưng vẫn commit offset (không reprocess)
                        self.consumer.commit()
                        logger.info(
                            f"⏭️  [{self.process_name}] Skipped duplicate message - "
                            f"Total duplicates: {duplicate_count}"
                        )
                        continue
                    
                    # ========================
                    # PROCESS MESSAGE
                    # ========================
                    success = self._process_with_retry(message.value, max_retries=3)
                    
                    if success:
                        # ✅ SUCCESS
                        success_count += 1
                        
                        # Mark as processed (idempotency)
                        self._mark_as_processed(message.value)
                        
                        # Commit offset
                        self.consumer.commit()
                        
                        logger.info(
                            f"✅ [{self.process_name}] Successfully processed - "
                            f"Offset: {message.offset}, "
                            f"Success rate: {success_count}/{message_count - duplicate_count}"
                        )
                    else:
                        # ❌ FAILED after all retries
                        failed_count += 1
                        
                        # Send to DLQ
                        self._send_to_dlq(
                            message.value,
                            "Failed after 3 retry attempts"
                        )
                        
                        # Commit offset để không reprocess
                        self.consumer.commit()
                        
                        logger.error(
                            f"❌ [{self.process_name}] Processing failed permanently - "
                            f"Offset: {message.offset}, "
                            f"Failed: {failed_count}"
                        )
                
                except json.JSONDecodeError as e:
                    failed_count += 1
                    logger.error(
                        f"❌ [{self.process_name}] Invalid JSON: {e}"
                    )
                    # Commit để skip invalid message
                    self.consumer.commit()
                
                except Exception as e:
                    failed_count += 1
                    logger.error(
                        f"💥 [{self.process_name}] Unexpected error: {e}",
                        exc_info=True
                    )
                    # DON'T commit - sẽ reprocess
        
        except KeyboardInterrupt:
            logger.info(f"🛑 [{self.process_name}] Shutting down gracefully...")
        
        except Exception as e:
            logger.error(f"💥 [{self.process_name}] Fatal error: {e}", exc_info=True)
        
        finally:
            # Print summary
            logger.info(
                f"\n{'='*70}\n"
                f"📊 [{self.process_name}] FINAL STATISTICS:\n"
                f"   Total messages received: {message_count}\n"
                f"   ✅ Successfully processed: {success_count}\n"
                f"   ⚠️  Duplicates skipped: {duplicate_count}\n"
                f"   ❌ Failed: {failed_count}\n"
                f"   Success rate: {success_count/(message_count-duplicate_count)*100:.1f}%\n"
                f"{'='*70}"
            )
            self.close()
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info(f"✅ [{self.process_name}] Consumer closed successfully")
            except Exception as e:
                logger.error(f"❌ [{self.process_name}] Error closing consumer: {e}")