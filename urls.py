# urls.py at project root level (not inside /app)
from django.urls import path, include
from rest_framework_simplejwt.views import (
    TokenObtainPairView,
    TokenRefreshView,
)

urlpatterns = [
    # JWT endpoints
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),

    # App API endpoints
    path('api/core/', include('app.core.api.urls')),
    path('api/deliveries/', include('app.deliveries.api.urls')),
    path('api/vehicles/', include('app.vehicles.api.urls')),  # keep this as is
    path('api/customers/', include('app.customers.api.urls')),  # keep only this
]
