# ---------- deliveries/api/serializers.py ----------
from rest_framework import serializers
from app.deliveries.models import Delivery, Route

class RouteSerializer(serializers.ModelSerializer):
    class Meta:
        model = Route
        fields = '__all__'

class DeliverySerializer(serializers.ModelSerializer):
    class Meta:
        model = Delivery
        fields = '__all__'