"""
URL configuration for CinéExplorer project.

Phase 3 - T3.3: Routes principales
"""

from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('movies.urls')),
]
