# ---------- deliveries/api/views.py ----------
from rest_framework import viewsets
from app.deliveries.models import Delivery, Route
from .serializers import DeliverySerializer, RouteSerializer

class DeliveryViewSet(viewsets.ModelViewSet):
    queryset = Delivery.objects.all()
    serializer_class = DeliverySerializer

class RouteViewSet(viewsets.ModelViewSet):
    queryset = Route.objects.all()
    serializer_class = RouteSerializer
