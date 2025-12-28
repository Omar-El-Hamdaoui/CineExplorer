"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - Views                                                   ║
║                                                                                ║
║     Phase 3 - T3.3: Vues Django avec intégration multi-bases                  ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from django.shortcuts import render
from django.http import JsonResponse
from django.views import View

from .services import sqlite_service, mongo_service


def home(request):
    """Page d'accueil avec statistiques"""
    context = {
        'title': 'CinéExplorer - Accueil',
    }
    
    try:
        # Statistiques SQLite
        context['sqlite_stats'] = sqlite_service.get_stats()
        context['sqlite_connected'] = True
    except Exception as e:
        context['sqlite_stats'] = {}
        context['sqlite_connected'] = False
        context['sqlite_error'] = str(e)
    
    try:
        # Statistiques MongoDB
        context['mongo_stats'] = mongo_service.get_stats()
        context['mongo_connected'] = mongo_service.is_connected()
        context['replica_status'] = mongo_service.get_replica_status()
    except Exception as e:
        context['mongo_stats'] = {}
        context['mongo_connected'] = False
        context['mongo_error'] = str(e)
    
    try:
        # Top films
        context['top_movies'] = mongo_service.get_top_movies(limit=10, min_votes=100000)
    except:
        context['top_movies'] = []
    
    return render(request, 'movies/home.html', context)


def stats_view(request):
    """Page de statistiques détaillées"""
    context = {
        'title': 'CinéExplorer - Statistiques',
    }
    
    try:
        # SQLite Stats
        context['sqlite_stats'] = sqlite_service.get_stats()
        context['genres_stats'] = sqlite_service.get_movies_by_genre_stats()[:15]
        context['decades_stats'] = sqlite_service.get_movies_by_decade()
        context['rating_distribution'] = sqlite_service.get_rating_distribution()
        context['top_actors'] = sqlite_service.get_top_actors(limit=10)
    except Exception as e:
        context['sqlite_error'] = str(e)
    
    try:
        # MongoDB Stats
        context['mongo_stats'] = mongo_service.get_stats()
        context['mongo_genres_stats'] = mongo_service.get_genres_stats()[:15]
        context['mongo_decades_stats'] = mongo_service.get_movies_by_decade()
        context['top_directors'] = mongo_service.get_top_directors(limit=10)
    except Exception as e:
        context['mongo_error'] = str(e)
    
    return render(request, 'movies/stats.html', context)


def api_stats(request):
    """API JSON pour les statistiques (pour tests)"""
    data = {
        'sqlite': {},
        'mongodb': {},
        'replica_set': {}
    }
    
    try:
        data['sqlite'] = sqlite_service.get_stats()
        data['sqlite']['connected'] = True
    except Exception as e:
        data['sqlite']['connected'] = False
        data['sqlite']['error'] = str(e)
    
    try:
        data['mongodb'] = mongo_service.get_stats()
        data['mongodb']['connected'] = mongo_service.is_connected()
        data['replica_set'] = mongo_service.get_replica_status()
    except Exception as e:
        data['mongodb']['connected'] = False
        data['mongodb']['error'] = str(e)
    
    return JsonResponse(data)


def api_test_databases(request):
    """API de test des connexions aux bases de données"""
    results = {
        'timestamp': None,
        'sqlite': {
            'connected': False,
            'test_query': None,
        },
        'mongodb': {
            'connected': False,
            'replica_set': None,
            'test_query': None,
        }
    }
    
    from datetime import datetime
    results['timestamp'] = datetime.now().isoformat()
    
    # Test SQLite
    try:
        stats = sqlite_service.get_stats()
        results['sqlite']['connected'] = True
        results['sqlite']['test_query'] = f"Movies count: {stats.get('movies_count', 0)}"
        results['sqlite']['stats'] = stats
    except Exception as e:
        results['sqlite']['error'] = str(e)
    
    # Test MongoDB
    try:
        if mongo_service.is_connected():
            results['mongodb']['connected'] = True
            results['mongodb']['replica_set'] = mongo_service.get_replica_status()
            
            # Test query
            movie = mongo_service.db.movies_complete.find_one({'rating.votes': {'$gt': 1000000}})
            if movie:
                results['mongodb']['test_query'] = f"Found: {movie.get('title')} ({movie.get('year')})"
            
            results['mongodb']['stats'] = mongo_service.get_stats()
    except Exception as e:
        results['mongodb']['error'] = str(e)
    
    return JsonResponse(results, json_dumps_params={'indent': 2})
