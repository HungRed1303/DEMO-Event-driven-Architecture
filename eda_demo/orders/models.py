from django.db import models

# Create your models here.
from django.db import models

class Order(models.Model):
    product = models.CharField(max_length=200)
    quantity = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
