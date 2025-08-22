# app/customers/models.py
from django.db import models
from django.contrib.gis.db import models as gis_models  # ✅ for PostGIS fields

class Customer(models.Model):
    name = models.CharField(max_length=150)
    email = models.EmailField(unique=True, blank=True, null=True)
    phone = models.CharField(max_length=20, blank=True, null=True)
    address = models.TextField(blank=True, null=True)
    location = gis_models.PointField(geography=True, srid=4326, blank=True, null=True)  # ✅ matches DB
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name
