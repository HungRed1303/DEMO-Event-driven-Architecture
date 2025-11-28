from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from django.shortcuts import render
from .models import Order
from .serializers import OrderSerializer
from .kafka_producer import publish_order_event
import logging

# Thiết lập Logger (nên dùng logger của Django)
logger = logging.getLogger(__name__)

class OrderCreateView(APIView):
    """
    View để xử lý việc tạo đơn hàng mới và phát hành sự kiện Kafka.
    """
    def post(self, request):
        serializer = OrderSerializer(data=request.data)
        
        if not serializer.is_valid(raise_exception=True):
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        try:
            order = serializer.save() 
            logger.info(f"Order {order.id} saved successfully to the database.")
        except Exception as e:
            logger.error(f"Error saving order to DB: {e}")
            return Response(
                {'message': 'Failed to save order to database.', 'error': str(e)}, 
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        try:
            publish_order_event(serializer.data) 
            logger.info(f"Order event for order {order.id} published to Kafka.")
        except Exception as e:
            logger.error(f"Failed to publish order event for order {order.id} to Kafka: {e}")
            pass

        # 4. Trả về Phản hồi Thành công
        return Response(
            {
                'message': 'Order created & event published!', 
                'data': OrderSerializer(order).data 
            }, 
            status=status.HTTP_201_CREATED
        )