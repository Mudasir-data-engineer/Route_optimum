# app/vehicles/models.py
from django.db import models
from app.core.models import User

class Vehicle(models.Model):
    plate_number = models.CharField(max_length=50, unique=True)  # ✅ matches DB
    capacity = models.PositiveIntegerField()  # ✅ CHECK > 0 handled in logic
    driver = models.OneToOneField(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        limit_choices_to={'role': 'driver'}  # ✅ only drivers selectable
    )
    status = models.CharField(
        max_length=50,
        choices=[
            ('available', 'Available'),
            ('on_route', 'On Route'),
            ('maintenance', 'Maintenance')
        ],
        default='available'
    )

    def __str__(self):
        return f"{self.plate_number} ({self.status})"
