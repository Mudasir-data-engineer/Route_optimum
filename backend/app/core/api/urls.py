# app/core/api/urls.py
from django.urls import path, include
from rest_framework.routers import DefaultRouter
from app.core.views_auth import register_view, login_view, logout_view
from .views import DeliveryViewSet

router = DefaultRouter()
router.register(r'deliveries', DeliveryViewSet, basename='delivery')

urlpatterns = [
    path('register/', register_view, name='register'),
    path('login/', login_view, name='login'),
    path('logout/', logout_view, name='logout'),
    path('', include(router.urls)),
]
