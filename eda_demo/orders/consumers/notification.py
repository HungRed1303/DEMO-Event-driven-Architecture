import os
import django
import logging

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'eda_demo.settings')
django.setup()

from django.conf import settings
from orders.consumers.base_consumer import BaseKafkaConsumer
from orders.tasks import send_notification

logger = logging.getLogger(__name__)

class NotificationConsumer(BaseKafkaConsumer):
    """Consumer để gửi notifications"""
    
    def __init__(self):
        super().__init__(
            topic=settings.KAFKA_TOPIC_ORDERS,
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-notification",
            process_name="Notification Service"
        )
    
    def process_message(self, message_data):
        """
        Xử lý message để gửi notification
        
        Args:
            message_data: Order data from Kafka
        
        Returns:
            bool: True if successful
        """
        try:
            order_id = message_data.get('id')
            product = message_data.get('product')
            
            logger.info(
                f"[{self.process_name}] Processing notification for order {order_id} - "
                f"Product: {product}"
            )
            
            # Trigger Celery task
            send_notification.delay(message_data)
            
            logger.info(f"[{self.process_name}] Queued notification for order {order_id}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.process_name}] Error: {e}", exc_info=True)
            return False

if __name__ == '__main__':
    consumer = NotificationConsumer()
    consumer.start()