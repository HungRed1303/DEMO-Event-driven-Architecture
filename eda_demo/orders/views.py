from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.shortcuts import render
from django.db import transaction
import logging
import uuid

from .models import Order
from .serializers import OrderSerializer
from .tasks import publish_order_event_async

logger = logging.getLogger(__name__)

class OrderCreateView(APIView):
    """
    View để xử lý việc tạo đơn hàng mới và phát hành sự kiện Kafka bất đồng bộ.
    """
    
    def post(self, request):
        # Validate dữ liệu đầu vào
        serializer = OrderSerializer(data=request.data)
        
        if not serializer.is_valid():
            logger.warning(f"Invalid order data: {serializer.errors}")
            return Response(
                {'message': 'Invalid data', 'errors': serializer.errors}, 
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Tạo idempotency key từ client hoặc tự generate
        idempotency_key = request.headers.get('X-Idempotency-Key') or str(uuid.uuid4())
        
        try:
            # Sử dụng transaction để đảm bảo atomicity
            with transaction.atomic():
                # Kiểm tra idempotency - tránh tạo đơn hàng trùng
                existing_order = Order.objects.filter(
                    idempotency_key=idempotency_key
                ).first()
                
                if existing_order:
                    logger.info(f"Duplicate request detected with key: {idempotency_key}")
                    return Response(
                        {
                            'message': 'Order already exists',
                            'data': OrderSerializer(existing_order).data
                        },
                        status=status.HTTP_200_OK
                    )
                
                # Lưu đơn hàng vào database
                order = serializer.save(
                    idempotency_key=idempotency_key,
                    status='pending'
                )
                logger.info(f"Order {order.id} created successfully in database")
            
            # Gửi event bất đồng bộ qua Celery
            # Điều này sẽ không block request, xử lý trong background
            task = publish_order_event_async.delay(order.id)
            logger.info(f"Async task {task.id} created for order {order.id}")
            
            # Trả về response ngay lập tức
            return Response(
                {
                    'message': 'Order created successfully. Processing in background.',
                    'data': OrderSerializer(order).data,
                    'task_id': str(task.id)
                },
                status=status.HTTP_201_CREATED
            )
            
        except Exception as e:
            logger.error(f"Error creating order: {e}", exc_info=True)
            return Response(
                {
                    'message': 'Failed to create order',
                    'error': str(e)
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class OrderStatusView(APIView):
    """
    View để kiểm tra trạng thái xử lý của đơn hàng
    """
    
    def get(self, request, order_id):
        try:
            order = Order.objects.get(id=order_id)
            
            return Response({
                'order_id': order.id,
                'status': order.status,
                'event_published': order.event_published,
                'inventory_processed': order.inventory_processed,
                'notification_sent': order.notification_sent,
                'analytics_recorded': order.analytics_recorded,
                'created_at': order.created_at,
                'updated_at': order.updated_at,
            }, status=status.HTTP_200_OK)
            
        except Order.DoesNotExist:
            return Response(
                {'message': 'Order not found'},
                status=status.HTTP_404_NOT_FOUND
            )


class HealthCheckView(APIView):
    """
    Health check endpoint để monitor service
    """
    
    def get(self, request):
        from .kafka_producer import get_producer
        from celery import current_app
        
        health_status = {
            'status': 'healthy',
            'services': {}
        }
        
        # Check Database
        try:
            Order.objects.count()
            health_status['services']['database'] = 'healthy'
        except Exception as e:
            health_status['services']['database'] = f'unhealthy: {str(e)}'
            health_status['status'] = 'degraded'
        
        # Check Kafka
        try:
            producer = get_producer()
            health_status['services']['kafka'] = 'healthy'
        except Exception as e:
            health_status['services']['kafka'] = f'unhealthy: {str(e)}'
            health_status['status'] = 'degraded'
        
        # Check Celery
        try:
            inspect = current_app.control.inspect()
            stats = inspect.stats()
            if stats:
                health_status['services']['celery'] = 'healthy'
            else:
                health_status['services']['celery'] = 'no workers'
                health_status['status'] = 'degraded'
        except Exception as e:
            health_status['services']['celery'] = f'unhealthy: {str(e)}'
            health_status['status'] = 'degraded'
        
        response_status = status.HTTP_200_OK if health_status['status'] == 'healthy' else status.HTTP_503_SERVICE_UNAVAILABLE
        
        return Response(health_status, status=response_status)