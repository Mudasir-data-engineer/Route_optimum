from django.urls import path, include

urlpatterns = [
    path('api/core/', include('app.core.api.urls')),  # 👈 add prefix here
]
