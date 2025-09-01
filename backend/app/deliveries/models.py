# app/deliveries/models.py
from django.db import models
from django.contrib.gis.db import models as gis_models
from app.customers.models import Customer
from app.routes.models import Route  # ✅ import Route from routes app


class Delivery(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    route = models.ForeignKey(Route, on_delete=models.SET_NULL, null=True, blank=True)  # ✅ uses the correct Route
    scheduled_date = models.DateField()
    status = models.CharField(
        max_length=50,
        default='pending',
        choices=[
            ('pending', 'Pending'),
            ('in_transit', 'In Transit'),
            ('delivered', 'Delivered'),
            ('failed', 'Failed'),
        ]
    )
    priority = models.CharField(
        max_length=50,
        default='normal',
        choices=[
            ('low', 'Low'),
            ('normal', 'Normal'),
            ('high', 'High'),
        ]
    )
    delivery_location = gis_models.PointField(geography=True, srid=4326, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Delivery {self.id} - {self.status}"
