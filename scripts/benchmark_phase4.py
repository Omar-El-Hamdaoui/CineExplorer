#!/usr/bin/env python3
"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - Benchmark Phase 4                                       ║
║                                                                                ║
║     Mesure des performances des requêtes et pages                             ║
║     Usage: python manage.py shell < scripts/benchmark_phase4.py               ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import statistics

# Configuration Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from movies.services.sqlite_service import sqlite_service
from movies.services.mongo_service import mongo_service


def measure_time(func, *args, iterations=5, **kwargs):
    """Mesure le temps d'exécution moyen d'une fonction"""
    times = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        times.append((end - start) * 1000)  # en ms
    return {
        'avg': round(statistics.mean(times), 2),
        'min': round(min(times), 2),
        'max': round(max(times), 2),
        'std': round(statistics.stdev(times), 2) if len(times) > 1 else 0
    }


def print_header(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_result(name, result, unit="ms"):
    print(f"  {name:<40} {result['avg']:>8.2f} {unit} (±{result['std']:.2f})")


def run_benchmarks():
    """Exécute tous les benchmarks"""
    
    print()
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 20 + "CINEEXPLORER - BENCHMARK" + " " * 24 + "║")
    print("║" + " " * 25 + "Phase 4 - Django" + " " * 27 + "║")
    print("╚" + "═" * 68 + "╝")
    
    results = {}
    
    # =========================================================================
    # 1. STATISTIQUES GÉNÉRALES
    # =========================================================================
    print_header("1. STATISTIQUES GÉNÉRALES")
    
    results['sqlite_stats'] = measure_time(sqlite_service.get_stats)
    print_result("SQLite: get_stats()", results['sqlite_stats'])
    
    results['mongo_stats'] = measure_time(mongo_service.get_stats)
    print_result("MongoDB: get_stats()", results['mongo_stats'])
    
    # =========================================================================
    # 2. LISTE DES FILMS
    # =========================================================================
    print_header("2. LISTE DES FILMS (20 films)")
    
    results['sqlite_movies_list'] = measure_time(
        sqlite_service.get_movies, limit=20, offset=0
    )
    print_result("SQLite: get_movies(limit=20)", results['sqlite_movies_list'])
    
    results['mongo_movies_list'] = measure_time(
        mongo_service.get_movies_complete, limit=20, skip=0
    )
    print_result("MongoDB: get_movies_complete(limit=20)", results['mongo_movies_list'])
    
    # =========================================================================
    # 3. LISTE AVEC FILTRES
    # =========================================================================
    print_header("3. LISTE AVEC FILTRES (genre=Drama, 2010-2020)")
    
    results['sqlite_filtered'] = measure_time(
        sqlite_service.get_movies, limit=20, offset=0, genre='Drama',
        year_min=2010, year_max=2020
    )
    print_result("SQLite: filtré par genre/année", results['sqlite_filtered'])
    
    results['mongo_filtered'] = measure_time(
        mongo_service.get_movies_complete, limit=20, skip=0, genre='Drama',
        year_min=2010, year_max=2020
    )
    print_result("MongoDB: filtré par genre/année", results['mongo_filtered'])
    
    # =========================================================================
    # 4. DÉTAIL D'UN FILM
    # =========================================================================
    print_header("4. DÉTAIL D'UN FILM (The Godfather)")
    
    # Trouver l'ID du Godfather
    godfather_id = "tt0068646"
    
    results['sqlite_detail'] = measure_time(
        sqlite_service.get_movie_by_id, godfather_id
    )
    print_result("SQLite: get_movie_by_id()", results['sqlite_detail'])
    
    results['mongo_detail'] = measure_time(
        mongo_service.get_movie_complete, godfather_id
    )
    print_result("MongoDB: get_movie_complete()", results['mongo_detail'])
    
    # =========================================================================
    # 5. RECHERCHE
    # =========================================================================
    print_header("5. RECHERCHE (query='godfather')")
    
    results['sqlite_search'] = measure_time(
        sqlite_service.search_movies, 'godfather', limit=20
    )
    print_result("SQLite: search_movies()", results['sqlite_search'])
    
    results['mongo_search'] = measure_time(
        mongo_service.search_movies, 'godfather', limit=20
    )
    print_result("MongoDB: search_movies()", results['mongo_search'])
    
    # =========================================================================
    # 6. AGRÉGATIONS (Statistiques)
    # =========================================================================
    print_header("6. AGRÉGATIONS (Page Statistiques)")
    
    results['sqlite_genres'] = measure_time(sqlite_service.get_movies_by_genre_stats)
    print_result("SQLite: get_movies_by_genre_stats()", results['sqlite_genres'])
    
    results['mongo_genres'] = measure_time(mongo_service.get_genres_stats)
    print_result("MongoDB: get_genres_stats()", results['mongo_genres'])
    
    results['sqlite_decades'] = measure_time(sqlite_service.get_movies_by_decade)
    print_result("SQLite: get_movies_by_decade()", results['sqlite_decades'])
    
    results['mongo_decades'] = measure_time(mongo_service.get_movies_by_decade)
    print_result("MongoDB: get_movies_by_decade()", results['mongo_decades'])
    
    results['sqlite_top_actors'] = measure_time(sqlite_service.get_top_actors, limit=10)
    print_result("SQLite: get_top_actors()", results['sqlite_top_actors'])
    
    results['mongo_top_directors'] = measure_time(mongo_service.get_top_directors, limit=10)
    print_result("MongoDB: get_top_directors()", results['mongo_top_directors'])
    
    # =========================================================================
    # 7. TOP FILMS
    # =========================================================================
    print_header("7. TOP FILMS (min 100K votes)")
    
    results['mongo_top'] = measure_time(
        mongo_service.get_top_movies, limit=10, min_votes=100000
    )
    print_result("MongoDB: get_top_movies()", results['mongo_top'])
    
    # =========================================================================
    # 8. FILMS SIMILAIRES
    # =========================================================================
    print_header("8. FILMS SIMILAIRES")
    
    results['mongo_similar'] = measure_time(
        mongo_service.get_similar_movies, godfather_id, limit=6
    )
    print_result("MongoDB: get_similar_movies()", results['mongo_similar'])
    
    # =========================================================================
    # RÉSUMÉ
    # =========================================================================
    print_header("RÉSUMÉ COMPARATIF SQLite vs MongoDB")
    
    comparisons = [
        ("Statistiques générales", 'sqlite_stats', 'mongo_stats'),
        ("Liste films (20)", 'sqlite_movies_list', 'mongo_movies_list'),
        ("Liste filtrée", 'sqlite_filtered', 'mongo_filtered'),
        ("Détail film", 'sqlite_detail', 'mongo_detail'),
        ("Recherche", 'sqlite_search', 'mongo_search'),
        ("Stats par genre", 'sqlite_genres', 'mongo_genres'),
        ("Films par décennie", 'sqlite_decades', 'mongo_decades'),
    ]
    
    print()
    print(f"  {'Opération':<30} {'SQLite':>10} {'MongoDB':>10} {'Ratio':>10}")
    print("  " + "-" * 62)
    
    for name, sqlite_key, mongo_key in comparisons:
        sqlite_time = results[sqlite_key]['avg']
        mongo_time = results[mongo_key]['avg']
        ratio = sqlite_time / mongo_time if mongo_time > 0 else 0
        winner = "SQLite" if sqlite_time < mongo_time else "MongoDB"
        print(f"  {name:<30} {sqlite_time:>8.2f}ms {mongo_time:>8.2f}ms {ratio:>8.2f}x ({winner})")
    
    # =========================================================================
    # TABLEAU FINAL
    # =========================================================================
    print_header("TABLEAU POUR LE RAPPORT")
    
    print()
    print("| Opération | SQLite (ms) | MongoDB (ms) | Gagnant |")
    print("|-----------|-------------|--------------|---------|")
    
    for name, sqlite_key, mongo_key in comparisons:
        sqlite_time = results[sqlite_key]['avg']
        mongo_time = results[mongo_key]['avg']
        winner = "SQLite" if sqlite_time < mongo_time else "MongoDB"
        print(f"| {name} | {sqlite_time:.2f} | {mongo_time:.2f} | {winner} |")
    
    print()
    print("=" * 70)
    print("  Benchmark terminé!")
    print("=" * 70)
    print()
    
    return results


if __name__ == "__main__":
    run_benchmarks()
