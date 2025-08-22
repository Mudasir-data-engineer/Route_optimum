# app/routes/models.py
from django.db import models
from django.contrib.gis.db import models as gis_models
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
