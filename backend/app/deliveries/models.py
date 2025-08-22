# app/deliveries/models.py
from django.db import models
from django.contrib.gis.db import models as gis_models
from app.customers.models import Customer
from app.vehicles.models import Vehicle
from app.core.models import User

class Route(models.Model):
    vehicle = models.ForeignKey(Vehicle, on_delete=models.SET_NULL, null=True, blank=True)
    driver = models.ForeignKey(User, limit_choices_to={'role': 'driver'}, on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(
        max_length=50,
        default='planned',
        choices=[
            ('planned', 'Planned'),
            ('in_progress', 'In Progress'),
            ('completed', 'Completed'),
        ]
    )
    route_path = gis_models.LineStringField(geography=True, srid=4326, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Route {self.id} - {self.status}"

class Delivery(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    route = models.ForeignKey(Route, on_delete=models.SET_NULL, null=True, blank=True)
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
