# ---------- deliveries/models.py ----------
from django.db import models
from app.customers.models import Customer
from app.vehicles.models import Vehicle
from app.core.models import User

class Route(models.Model):
    name = models.CharField(max_length=100)
    optimized = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name

class Delivery(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    address = models.CharField(max_length=255)
    scheduled_date = models.DateField()
    delivered = models.BooleanField(default=False)
    assigned_driver = models.ForeignKey(User, limit_choices_to={'role': 'driver'}, on_delete=models.SET_NULL, null=True)
    route = models.ForeignKey(Route, on_delete=models.SET_NULL, null=True, blank=True)

    def __str__(self):
        return f"Delivery to {self.customer.name} on {self.scheduled_date}"