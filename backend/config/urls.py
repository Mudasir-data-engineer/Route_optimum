# backend/config/urls.py

from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
    path('admin/', admin.site.urls),

    # JWT token endpoints
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),

    # App-specific API endpoints
    path('api/core/', include('app.core.api.urls')),
    path('api/customers/', include('app.customers.api.urls')),
    path('api/vehicles/', include('app.vehicles.api.urls')),
    path('api/deliveries/', include('app.deliveries.api.urls')),
    path('api/routes/', include('app.routes.urls')),  # ✅ routes add here
]

if settings.DEBUG:
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATICFILES_DIRS[0])
