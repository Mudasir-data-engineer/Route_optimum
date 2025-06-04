# ---------- vehicles/api/serializers.py ----------
from rest_framework import serializers
from app.vehicles.models import Vehicle

class VehicleSerializer(serializers.ModelSerializer):
    class Meta:
        model = Vehicle
        fields = '__all__'
