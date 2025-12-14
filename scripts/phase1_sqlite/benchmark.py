#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
CineExplorer - BENCHMARK.PY (T1.4 - Indexation et Benchmark)
================================================================================

Utilité:
  1. Mesurer le temps d'exécution de chaque requête (SANS index)
  2. Analyser les plans d'exécution avec EXPLAIN QUERY PLAN
  3. Créer des index pertinents
  4. Mesurer à nouveau (AVEC index)
  5. Calculer le gain en performance

Auteur: Équipe CineExplorer
Date: 2025-12-14
Version: 1.0

Usage:
  python scripts/benchmark.py
"""

import sqlite3
import time
import os
from pathlib import Path
from typing import Tuple, List, Dict
from queries import (
    query_actor_filmography,
    query_top_n_films,
    query_multi_role_actors,
    query_collaborations,
    query_popular_genres,
    query_career_evolution,
    query_top_films_per_genre,
    query_breakthrough_careers,
    query_most_prolific_directors,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH = Path(__file__).parent.parent.parent / "data" / "imdb.db"

# Paramètres des requêtes (les clés doivent correspondre aux noms des fonctions)
QUERY_PARAMS = {
    "query_actor_filmography": {"actor_name": "Tom Hanks"},
    "query_top_n_films": {"genre": "Drama", "year_start": 2010, "year_end": 2024, "n": 10},
    "query_multi_role_actors": {},
    "query_collaborations": {"actor_name": "Steven Spielberg"},
    "query_popular_genres": {"min_rating": 7.0, "min_count": 50},
    "query_career_evolution": {"actor_name": "Meryl Streep"},
    "query_top_films_per_genre": {"top_n": 5},
    "query_breakthrough_careers": {"threshold_votes": 200000},
    "query_most_prolific_directors": {"min_films": 10},
}


# ============================================================================
# COULEURS
# ============================================================================

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


# ============================================================================
# CLASSE BENCHMARK
# ============================================================================

class QueryBenchmark:
    """Effectue le benchmarking des requêtes"""

    def __init__(self, db_path: Path):
        """Initialiser"""
        self.db_path = db_path
        self.conn = None
        self.results = {}
        self.query_plans = {}

    def connect(self) -> bool:
        """Établir la connexion"""
        try:
            self.conn = sqlite3.connect(str(self.db_path))
            # Désactiver les indexes existants pour mesurer SANS index
            return True
        except Exception as e:
            print(f"{Colors.FAIL}❌ Erreur de connexion: {e}{Colors.ENDC}")
            return False

    def disconnect(self) -> None:
        """Fermer la connexion"""
        if self.conn:
            self.conn.close()

    def get_query_plan(self, query_name: str, sql: str) -> str:
        """Obtenir le plan d'exécution d'une requête"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {sql}")
            plan = "\n".join(str(row) for row in cursor.fetchall())
            return plan
        except Exception as e:
            return f"Erreur: {e}"

    def measure_query_time(self, query_func, params: dict, iterations: int = 3) -> float:
        """
        Mesurer le temps d'exécution d'une requête (moyenne sur plusieurs itérations)

        Args:
            query_func: Fonction de requête
            params: Paramètres de la requête
            iterations: Nombre d'itérations

        Returns:
            Temps moyen en millisecondes
        """
        times = []

        for _ in range(iterations):
            start = time.perf_counter()
            try:
                query_func(self.conn, **params)
            except Exception as e:
                print(f"{Colors.FAIL}❌ Erreur d'exécution: {e}{Colors.ENDC}")
                return -1
            end = time.perf_counter()
            times.append((end - start) * 1000)  # Convertir en ms

        return sum(times) / len(times)

    def benchmark_without_indexes(self) -> Dict:
        """
        Mesurer les temps d'exécution SANS index
        """
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}🔍 PHASE 1: BENCHMARK SANS INDEX{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

        queries = [
            ("1️⃣  Actor Filmography", query_actor_filmography),
            ("2️⃣  Top N Films", query_top_n_films),
            ("3️⃣  Multi-Role Actors", query_multi_role_actors),
            ("4️⃣  Collaborations", query_collaborations),
            ("5️⃣  Popular Genres", query_popular_genres),
            ("6️⃣  Career Evolution", query_career_evolution),
            ("7️⃣  Top Films per Genre", query_top_films_per_genre),
            ("8️⃣  Breakthrough Careers", query_breakthrough_careers),
            ("9️⃣  Most Prolific Directors", query_most_prolific_directors),
        ]

        results = {}

        for query_label, query_func in queries:
            query_key = query_func.__name__
            params = QUERY_PARAMS[query_key]

            print(f"⏳ {query_label:30s}", end=" ", flush=True)

            time_ms = self.measure_query_time(query_func, params, iterations=3)
            results[query_label] = time_ms

            print(f"✅ {time_ms:8.2f} ms")

        return results

    def create_indexes(self) -> None:
        """
        Créer les indexes pertinents
        """
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}🔧 PHASE 2: CRÉATION DES INDEXES{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

        indexes = [
            ("idx_persons_name", "CREATE INDEX IF NOT EXISTS idx_persons_name ON PERSONS(name)"),
            ("idx_movies_year", "CREATE INDEX IF NOT EXISTS idx_movies_year ON MOVIES(start_year)"),
            ("idx_movies_type", "CREATE INDEX IF NOT EXISTS idx_movies_type ON MOVIES(title_type)"),
            ("idx_ratings_movie", "CREATE INDEX IF NOT EXISTS idx_ratings_movie ON RATINGS(movie_id)"),
            ("idx_ratings_votes", "CREATE INDEX IF NOT EXISTS idx_ratings_votes ON RATINGS(num_votes)"),
            ("idx_genres_movie", "CREATE INDEX IF NOT EXISTS idx_genres_movie ON GENRES(movie_id)"),
            ("idx_genres_name", "CREATE INDEX IF NOT EXISTS idx_genres_name ON GENRES(genre)"),
            ("idx_principals_movie", "CREATE INDEX IF NOT EXISTS idx_principals_movie ON PRINCIPALS(movie_id)"),
            ("idx_principals_person", "CREATE INDEX IF NOT EXISTS idx_principals_person ON PRINCIPALS(person_id)"),
            ("idx_directors_movie", "CREATE INDEX IF NOT EXISTS idx_directors_movie ON DIRECTORS(movie_id)"),
            ("idx_directors_person", "CREATE INDEX IF NOT EXISTS idx_directors_person ON DIRECTORS(person_id)"),
            ("idx_writers_movie", "CREATE INDEX IF NOT EXISTS idx_writers_movie ON WRITERS(movie_id)"),
            ("idx_writers_person", "CREATE INDEX IF NOT EXISTS idx_writers_person ON WRITERS(person_id)"),
            ("idx_characters_movie", "CREATE INDEX IF NOT EXISTS idx_characters_movie ON CHARACTERS(movie_id)"),
            ("idx_characters_person", "CREATE INDEX IF NOT EXISTS idx_characters_person ON CHARACTERS(person_id)"),
            ("idx_episodes_parent", "CREATE INDEX IF NOT EXISTS idx_episodes_parent ON EPISODES(parent_movie_id)"),
            ("idx_knownformovies_person",
             "CREATE INDEX IF NOT EXISTS idx_knownformovies_person ON KNOWNFORMOVIES(person_id)"),
            ("idx_knownformovies_movie",
             "CREATE INDEX IF NOT EXISTS idx_knownformovies_movie ON KNOWNFORMOVIES(movie_id)"),
            ("idx_professions_person", "CREATE INDEX IF NOT EXISTS idx_professions_person ON PROFESSIONS(person_id)"),
            ("idx_titles_movie", "CREATE INDEX IF NOT EXISTS idx_titles_movie ON TITLES(movie_id)"),
        ]

        try:
            cursor = self.conn.cursor()

            for idx_name, sql in indexes:
                cursor.execute(sql)
                print(f"✅ {idx_name}")

            self.conn.commit()
            print(f"\n{Colors.OKGREEN}✅ {len(indexes)} indexes créés!{Colors.ENDC}")

        except Exception as e:
            print(f"{Colors.FAIL}❌ Erreur: {e}{Colors.ENDC}")

    def benchmark_with_indexes(self) -> Dict:
        """
        Mesurer les temps d'exécution AVEC index
        """
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}🚀 PHASE 3: BENCHMARK AVEC INDEX{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

        queries = [
            ("1️⃣  Actor Filmography", query_actor_filmography),
            ("2️⃣  Top N Films", query_top_n_films),
            ("3️⃣  Multi-Role Actors", query_multi_role_actors),
            ("4️⃣  Collaborations", query_collaborations),
            ("5️⃣  Popular Genres", query_popular_genres),
            ("6️⃣  Career Evolution", query_career_evolution),
            ("7️⃣  Top Films per Genre", query_top_films_per_genre),
            ("8️⃣  Breakthrough Careers", query_breakthrough_careers),
            ("9️⃣  Most Prolific Directors", query_most_prolific_directors),
        ]

        results = {}

        for query_label, query_func in queries:
            query_key = query_func.__name__
            params = QUERY_PARAMS[query_key]

            print(f"⏳ {query_label:30s}", end=" ", flush=True)

            time_ms = self.measure_query_time(query_func, params, iterations=3)
            results[query_label] = time_ms

            print(f"✅ {time_ms:8.2f} ms")

        return results

    def get_db_size(self) -> float:
        """Obtenir la taille de la base de données en MB"""
        if self.db_path.exists():
            return self.db_path.stat().st_size / (1024 * 1024)
        return 0

    def print_benchmark_table(self, without_idx: Dict, with_idx: Dict) -> None:
        """Afficher un tableau de benchmark"""
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}📊 RÉSULTATS FINAUX{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

        # En-têtes
        print(f"{'Requête':<30} {'Sans index':>12} {'Avec index':>12} {'Gain':>10}")
        print("-" * 80)

        total_without = 0
        total_with = 0

        for query_label in without_idx.keys():
            time_without = without_idx[query_label]
            time_with = with_idx[query_label]

            total_without += time_without
            total_with += time_with

            if time_without > 0:
                gain = ((time_without - time_with) / time_without) * 100
            else:
                gain = 0

            gain_color = Colors.OKGREEN if gain > 0 else Colors.WARNING

            print(
                f"{query_label:<30} {time_without:>10.2f} ms {time_with:>10.2f} ms {gain_color}{gain:>8.1f}%{Colors.ENDC}")

        print("-" * 80)

        if total_without > 0:
            total_gain = ((total_without - total_with) / total_without) * 100
        else:
            total_gain = 0

        print(
            f"{'TOTAL':<30} {total_without:>10.2f} ms {total_with:>10.2f} ms {Colors.OKGREEN}{total_gain:>8.1f}%{Colors.ENDC}")
        print()

    def print_size_comparison(self, size_without: float, size_with: float) -> None:
        """Afficher la comparaison de tailles"""
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}💾 TAILLE DE LA BASE DE DONNÉES{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

        size_increase = size_with - size_without
        size_increase_pct = (size_increase / size_without) * 100 if size_without > 0 else 0

        print(f"Sans index: {size_without:>10.2f} MB")
        print(f"Avec index: {size_with:>10.2f} MB")
        print(f"Augmentation: {size_increase:>6.2f} MB ({size_increase_pct:.1f}%)")
        print()

    def run_full_benchmark(self) -> None:
        """Exécuter le benchmark complet"""

        if not self.connect():
            return

        try:
            # PHASE 1: Benchmark SANS index
            without_idx = self.benchmark_without_indexes()
            size_without = self.get_db_size()

            # PHASE 2: Créer les indexes
            self.create_indexes()
            size_with = self.get_db_size()

            # PHASE 3: Benchmark AVEC index
            with_idx = self.benchmark_with_indexes()

            # AFFICHER LES RÉSULTATS
            self.print_benchmark_table(without_idx, with_idx)
            self.print_size_comparison(size_without, size_with)

            # Résumé
            print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
            print(f"{Colors.BOLD}{Colors.OKGREEN}✅ BENCHMARK COMPLET TERMINÉ!{Colors.ENDC}")
            print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

        finally:
            self.disconnect()


# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    """Fonction principale"""

    print(f"\n{Colors.BOLD}{Colors.OKBLUE}")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "🎬 CINEEXPLORER - T1.4 INDEXATION ET BENCHMARK".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝")
    print(Colors.ENDC)

    if not DB_PATH.exists():
        print(f"{Colors.FAIL}❌ Base de données introuvable: {DB_PATH}{Colors.ENDC}")
        return 1

    benchmark = QueryBenchmark(DB_PATH)
    benchmark.run_full_benchmark()

    return 0


# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == '__main__':
    import sys

    sys.exit(main())