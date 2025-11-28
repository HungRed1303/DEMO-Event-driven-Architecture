from django.db import models

class Order(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]
    
    product = models.CharField(max_length=200)
    quantity = models.IntegerField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # Event tracking
    event_published = models.BooleanField(default=False)
    event_published_at = models.DateTimeField(null=True, blank=True)
    
    # Processing tracking
    inventory_processed = models.BooleanField(default=False)
    notification_sent = models.BooleanField(default=False)
    analytics_recorded = models.BooleanField(default=False)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    # Idempotency key để tránh xử lý trùng
    idempotency_key = models.CharField(max_length=100, unique=True, null=True, blank=True)
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['created_at']),
            models.Index(fields=['idempotency_key']),
        ]
    
    def __str__(self):
        return f"Order {self.id} - {self.product} (x{self.quantity})"