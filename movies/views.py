"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - Views                                                   ║
║                                                                                ║
║     Phase 4: Interface Web Django Complète                                    ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from django.shortcuts import render
from django.http import JsonResponse
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

from .services import sqlite_service, mongo_service

import logging
logger = logging.getLogger(__name__)


# =============================================================================
# PAGE D'ACCUEIL
# =============================================================================

def home(request):
    """Page d'accueil avec statistiques"""
    context = {
        'title': 'CinéExplorer - Accueil',
    }
    
    try:
        context['sqlite_stats'] = sqlite_service.get_stats()
        context['sqlite_connected'] = True
    except Exception as e:
        context['sqlite_stats'] = {}
        context['sqlite_connected'] = False
        context['sqlite_error'] = str(e)
    
    try:
        context['mongo_stats'] = mongo_service.get_stats()
        context['mongo_connected'] = mongo_service.is_connected()
        context['replica_status'] = mongo_service.get_replica_status()
    except Exception as e:
        context['mongo_stats'] = {}
        context['mongo_connected'] = False
        context['mongo_error'] = str(e)
    
    try:
        context['top_movies'] = mongo_service.get_top_movies(limit=10, min_votes=100000)
    except:
        context['top_movies'] = []
    
    return render(request, 'movies/home.html', context)


# =============================================================================
# LISTE DES FILMS
# =============================================================================

def movies_list(request):
    """Liste des films avec pagination et filtres"""
    context = {
        'title': 'CinéExplorer - Films',
    }
    
    # Récupération des paramètres de filtrage
    genre = request.GET.get('genre', '')
    year_min = request.GET.get('year_min', '')
    year_max = request.GET.get('year_max', '')
    min_rating = request.GET.get('min_rating', '')
    sort_by = request.GET.get('sort', 'title')
    order = request.GET.get('order', 'asc')
    page = request.GET.get('page', 1)
    
    # Conversion des paramètres
    try:
        year_min = int(year_min) if year_min else None
    except ValueError:
        year_min = None
    
    try:
        year_max = int(year_max) if year_max else None
    except ValueError:
        year_max = None
    
    try:
        min_rating = float(min_rating) if min_rating else None
    except ValueError:
        min_rating = None
    
    # Mapping du tri
    sort_order = -1 if order == 'desc' else 1
    
    try:
        # Récupération des films depuis MongoDB (documents structurés)
        all_movies = mongo_service.get_movies_complete(
            limit=1000,
            skip=0,
            genre=genre if genre else None,
            year_min=year_min,
            year_max=year_max,
            min_rating=min_rating,
            sort_by=sort_by,
            sort_order=sort_order
        )
        
        # Pagination
        paginator = Paginator(all_movies, 20)  # 20 films par page
        
        try:
            movies = paginator.page(page)
        except PageNotAnInteger:
            movies = paginator.page(1)
        except EmptyPage:
            movies = paginator.page(paginator.num_pages)
        
        context['movies'] = movies
        context['total_count'] = len(all_movies)
        
    except Exception as e:
        logger.error(f"Error getting movies: {e}")
        context['movies'] = []
        context['error'] = str(e)
        context['total_count'] = 0
    
    # Récupération des genres pour le filtre
    try:
        all_genres = mongo_service.get_all_genres()
        logger.info(f"Genres récupérés: {len(all_genres)} genres")
        # Filtrer les valeurs None et trier
        context['all_genres'] = sorted([g for g in all_genres if g])
    except Exception as e:
        logger.error(f"Error getting genres: {e}")
        context['all_genres'] = []
    
    # Paramètres actuels pour le template
    context['current_filters'] = {
        'genre': genre,
        'year_min': year_min or '',
        'year_max': year_max or '',
        'min_rating': min_rating or '',
        'sort': sort_by,
        'order': order,
    }
    
    return render(request, 'movies/list.html', context)


# =============================================================================
# DÉTAIL D'UN FILM
# =============================================================================

def movie_detail(request, movie_id):
    """Détail complet d'un film"""
    context = {
        'title': 'CinéExplorer - Détail Film',
    }
    
    try:
        # Récupération du film depuis MongoDB (document structuré complet)
        movie = mongo_service.get_movie_complete(movie_id)
        
        if movie:
            context['movie'] = movie
            context['title'] = f"CinéExplorer - {movie.get('title', 'Film')}"
            
            # Films similaires (même genre)
            try:
                context['similar_movies'] = mongo_service.get_similar_movies(movie_id, limit=6)
            except:
                context['similar_movies'] = []
        else:
            context['error'] = "Film non trouvé"
            
    except Exception as e:
        context['error'] = str(e)
    
    return render(request, 'movies/detail.html', context)


# =============================================================================
# RECHERCHE
# =============================================================================

def search(request):
    """Page de recherche"""
    context = {
        'title': 'CinéExplorer - Recherche',
    }
    
    query = request.GET.get('q', '').strip()
    search_type = request.GET.get('type', 'all')  # all, movies, persons
    
    context['query'] = query
    context['search_type'] = search_type
    
    if query:
        try:
            # Recherche de films
            if search_type in ['all', 'movies']:
                context['movies_results'] = mongo_service.search_movies(query, limit=20)
            else:
                context['movies_results'] = []
            
            # Recherche de personnes
            if search_type in ['all', 'persons']:
                context['persons_results'] = mongo_service.search_persons(query, limit=20)
            else:
                context['persons_results'] = []
                
        except Exception as e:
            context['error'] = str(e)
            context['movies_results'] = []
            context['persons_results'] = []
    else:
        context['movies_results'] = []
        context['persons_results'] = []
    
    return render(request, 'movies/search.html', context)


# =============================================================================
# STATISTIQUES
# =============================================================================

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


# =============================================================================
# API ENDPOINTS
# =============================================================================

def api_stats(request):
    """API JSON pour les statistiques"""
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
    from datetime import datetime
    
    results = {
        'timestamp': datetime.now().isoformat(),
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
            
            movie = mongo_service.db.movies_complete.find_one({'rating.votes': {'$gt': 1000000}})
            if movie:
                results['mongodb']['test_query'] = f"Found: {movie.get('title')} ({movie.get('year')})"
            
            results['mongodb']['stats'] = mongo_service.get_stats()
    except Exception as e:
        results['mongodb']['error'] = str(e)
    
    return JsonResponse(results, json_dumps_params={'indent': 2})


def api_genres(request):
    """API pour lister les genres - DEBUG"""
    try:
        genres = mongo_service.get_all_genres()
        return JsonResponse({
            'count': len(genres),
            'genres': sorted([g for g in genres if g])
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
