from celery import shared_task
from django.utils import timezone
import logging
from .models import Order
from .kafka_producer import publish_order_event, publish_to_dlq

logger = logging.getLogger(__name__)

@shared_task(
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 1 minute
    autoretry_for=(Exception,)
)
def publish_order_event_async(self, order_id):
    """
    Asynchronously publish order event to Kafka
    
    Args:
        order_id: ID of the order to publish
    
    Returns:
        dict: Result of the operation
    """
    try:
        order = Order.objects.get(id=order_id)
        
        # Check if already published
        if order.event_published:
            logger.info(f"Order {order_id} event already published, skipping")
            return {'status': 'already_published', 'order_id': order_id}
        
        # Prepare order data
        order_data = {
            'id': order.id,
            'product': order.product,
            'quantity': order.quantity,
            'status': order.status,
            'created_at': order.created_at.isoformat(),
        }
        
        # Publish to Kafka
        publish_order_event(order_data)
        
        # Update order status
        order.event_published = True
        order.event_published_at = timezone.now()
        order.status = 'processing'
        order.save(update_fields=['event_published', 'event_published_at', 'status'])
        
        logger.info(f"Successfully published event for order {order_id}")
        
        return {
            'status': 'success',
            'order_id': order_id,
            'published_at': order.event_published_at.isoformat()
        }
        
    except Order.DoesNotExist:
        logger.error(f"Order {order_id} not found")
        return {'status': 'error', 'message': 'Order not found'}
        
    except Exception as e:
        logger.error(f"Error publishing event for order {order_id}: {e}", exc_info=True)
        
        # Try to send to DLQ after max retries
        if self.request.retries >= self.max_retries:
            try:
                order = Order.objects.get(id=order_id)
                order_data = {
                    'id': order.id,
                    'product': order.product,
                    'quantity': order.quantity,
                }
                publish_to_dlq(order_data, str(e))
                
                order.status = 'failed'
                order.save(update_fields=['status'])
                
            except Exception as dlq_error:
                logger.error(f"Failed to send to DLQ: {dlq_error}")
        
        raise

@shared_task
def process_inventory_update(order_data):
    """
    Simulate inventory update processing
    Can be replaced with actual inventory system integration
    """
    try:
        order_id = order_data.get('id')
        logger.info(f"[Inventory Task] Processing order {order_id}")
        
        # Simulate inventory update
        # In production, this would call an inventory service API
        
        # Update order status
        order = Order.objects.get(id=order_id)
        order.inventory_processed = True
        order.save(update_fields=['inventory_processed'])
        
        logger.info(f"[Inventory Task] Completed for order {order_id}")
        return {'status': 'success', 'order_id': order_id}
        
    except Exception as e:
        logger.error(f"[Inventory Task] Error: {e}", exc_info=True)
        raise

@shared_task
def send_notification(order_data):
    """
    Simulate sending notification
    Can be replaced with actual email/SMS service
    """
    try:
        order_id = order_data.get('id')
        logger.info(f"[Notification Task] Sending notification for order {order_id}")
        
        # Simulate sending email/SMS
        # In production, this would call SendGrid, Twilio, etc.
        
        # Update order status
        order = Order.objects.get(id=order_id)
        order.notification_sent = True
        order.save(update_fields=['notification_sent'])
        
        logger.info(f"[Notification Task] Sent for order {order_id}")
        return {'status': 'success', 'order_id': order_id}
        
    except Exception as e:
        logger.error(f"[Notification Task] Error: {e}", exc_info=True)
        raise

@shared_task
def record_analytics(order_data):
    """
    Simulate recording analytics
    Can be replaced with actual analytics service
    """
    try:
        order_id = order_data.get('id')
        logger.info(f"[Analytics Task] Recording data for order {order_id}")
        
        # Simulate analytics recording
        # In production, this would send to analytics platform
        
        # Update order status
        order = Order.objects.get(id=order_id)
        order.analytics_recorded = True
        
        # Mark as completed if all tasks done
        if order.inventory_processed and order.notification_sent and order.analytics_recorded:
            order.status = 'completed'
        
        order.save()
        
        logger.info(f"[Analytics Task] Recorded for order {order_id}")
        return {'status': 'success', 'order_id': order_id}
        
    except Exception as e:
        logger.error(f"[Analytics Task] Error: {e}", exc_info=True)
        raise