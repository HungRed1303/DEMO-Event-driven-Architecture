import os
import django
import logging

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'eda_demo.settings')
django.setup()

from django.conf import settings
from orders.consumers.base_consumer import BaseKafkaConsumer
from orders.tasks import process_inventory_update

logger = logging.getLogger(__name__)

class InventoryConsumer(BaseKafkaConsumer):
    """Consumer để xử lý cập nhật inventory"""
    
    def __init__(self):
        super().__init__(
            topic=settings.KAFKA_TOPIC_ORDERS,
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_PREFIX}-inventory",
            process_name="Inventory Service"
        )
    
    def process_message(self, message_data):
        """
        Xử lý message để cập nhật inventory
        
        Args:
            message_data: Order data from Kafka
        
        Returns:
            bool: True if successful
        """
        try:
            order_id = message_data.get('id')
            product = message_data.get('product')
            quantity = message_data.get('quantity')
            
            logger.info(
                f"[{self.process_name}] Processing order {order_id} - "
                f"Product: {product}, Quantity: {quantity}"
            )
            
            # Trigger Celery task cho xử lý bất đồng bộ
            # Điều này cho phép consumer xử lý nhiều message nhanh hơn
            process_inventory_update.delay(message_data)
            
            # Hoặc xử lý đồng bộ nếu cần:
            # from orders.models import Order
            # order = Order.objects.get(id=order_id)
            # order.inventory_processed = True
            # order.save()
            
            logger.info(f"[{self.process_name}] Queued inventory update for order {order_id}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.process_name}] Error: {e}", exc_info=True)
            return False

if __name__ == '__main__':
    consumer = InventoryConsumer()
    consumer.start()