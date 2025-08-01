# ---------- vehicles/api/views.py ----------
from rest_framework import viewsets
from app.vehicles.models import Vehicle
from .serializers import VehicleSerializer
from rest_framework import permissions

class VehicleViewSet(viewsets.ModelViewSet):
    queryset = Vehicle.objects.all()
    serializer_class = VehicleSerializer
    # Remove auth for now to ease development
    permission_classes = [permissions.IsAuthenticated]
