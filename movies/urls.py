"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - URLs                                                    ║
║                                                                                ║
║     Phase 4: Routes complètes de l'application                                ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from django.urls import path
from . import views

app_name = 'movies'

urlpatterns = [
    # Pages principales
    path('', views.home, name='home'),
    path('movies/', views.movies_list, name='movies_list'),
    path('movies/<str:movie_id>/', views.movie_detail, name='movie_detail'),
    path('search/', views.search, name='search'),
    path('stats/', views.stats_view, name='stats'),
    
    # API endpoints
    path('api/stats/', views.api_stats, name='api_stats'),
    path('api/test/', views.api_test_databases, name='api_test'),
    path('api/genres/', views.api_genres, name='api_genres'),
]
