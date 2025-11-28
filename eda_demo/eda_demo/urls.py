from django.contrib import admin
from django.urls import path
from orders.views import OrderCreateView, OrderStatusView, HealthCheckView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('orders/', OrderCreateView.as_view(), name='order-create'),
    path('orders/<int:order_id>/status/', OrderStatusView.as_view(), name='order-status'),
    path('health/', HealthCheckView.as_view(), name='health-check'),
]