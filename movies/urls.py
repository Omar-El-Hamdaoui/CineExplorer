"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - URLs                                                    ║
║                                                                                ║
║     Phase 3 - T3.3: Routes de l'application movies                            ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from django.urls import path
from . import views

app_name = 'movies'

urlpatterns = [
    # Pages principales
    path('', views.home, name='home'),
    path('stats/', views.stats_view, name='stats'),
    
    # API de test
    path('api/stats/', views.api_stats, name='api_stats'),
    path('api/test/', views.api_test_databases, name='api_test'),
]
