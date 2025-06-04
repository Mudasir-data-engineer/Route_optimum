# ---------- vehicles/models.py ----------
from django.db import models
from app.core.models import User

class Vehicle(models.Model):
    license_plate = models.CharField(max_length=20, unique=True)
    model = models.CharField(max_length=100)
    capacity = models.PositiveIntegerField()
    current_location = models.CharField(max_length=255)
    assigned_driver = models.OneToOneField(User, on_delete=models.SET_NULL, null=True, blank=True)

    def __str__(self):
        return f"{self.model} - {self.license_plate}"
