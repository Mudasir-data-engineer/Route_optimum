# ---------- deliveries/api/urls.py ----------
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import DeliveryViewSet, RouteViewSet

router = DefaultRouter()
router.register(r'deliveries', DeliveryViewSet)
router.register(r'routes', RouteViewSet)

urlpatterns = [
    path('', include(router.urls)),
]