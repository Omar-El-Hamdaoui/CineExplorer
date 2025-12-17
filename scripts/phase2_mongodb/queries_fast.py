#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║     🎬 CINEEXPLORER - T2.4: Les 9 Requêtes ULTRA-RAPIDES                 ║
║                                                                            ║
║          Exécutées sur movies_complete (PAS de $lookup!)                  ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝

Les MÊMES 9 requêtes SQL/MongoDB, mais sur movies_complete:
  • Pas de $lookup
  • Pas de $unwind lourd
  • Documents COMPLETS

Résultat: ULTRA-RAPIDE! ⚡

Usage:
  python3 T2.4_queries_fast.py

Temps estimé: 10-30 secondes (au lieu de 5-10 minutes!)
"""

from pymongo import MongoClient
import time
from datetime import datetime


class MovieQueriesFast:
    """Requêtes ULTRA-RAPIDES sur movies_complete"""

    def __init__(self):
        """Initialiser connexion"""
        print("📡 Connexion à MongoDB...")
        self.client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=60000)
        self.db = self.client["cineexplorer_flat"]
        self.stats = {}
        print("✅ Connecté!\n")

    # ========================================================================
    # REQUÊTE 1: Filmographie d'un acteur
    # ========================================================================

    def q1_actor_filmography(self, actor_name: str):
        """Requête 1: Filmographie d'un acteur"""
        pipeline = [
            {"$match": {"cast.name": {"$regex": actor_name, "$options": "i"}}},
            {"$project": {
                "_id": 1,
                "title": 1,
                "year": 1,
                "genres": 1,
                "rating": 1
            }},
            {"$sort": {"year": -1}},
            {"$limit": 100}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 2: Top N films par genre
    # ========================================================================

    def q2_top_n_films(self, genre: str, year_start: int, year_end: int, n: int = 10):
        """Requête 2: Top N films par genre et période"""
        pipeline = [
            {"$match": {
                "genres": genre,
                "year": {"$gte": year_start, "$lte": year_end},
                "rating.votes": {"$gt": 0}
            }},
            {"$sort": {"rating.average": -1}},
            {"$project": {
                "_id": 0,
                "title": 1,
                "year": 1,
                "rating": 1
            }},
            {"$limit": n}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 3: Acteurs multi-rôles
    # ========================================================================

    def q3_actors_multi_roles(self):
        """Requête 3: Acteurs avec plusieurs rôles"""
        pipeline = [
            {"$match": {"cast": {"$exists": True, "$ne": []}}},
            {"$unwind": "$cast"},
            {"$group": {
                "_id": {"name": "$cast.name", "title": "$title"},
                "roles_count": {"$sum": {"$size": "$cast.characters"}}
            }},
            {"$match": {"roles_count": {"$gt": 1}}},
            {"$project": {
                "_id": 0,
                "acteur": "$_id.name",
                "film": "$_id.title",
                "nombre_roles": "$roles_count"
            }},
            {"$sort": {"nombre_roles": -1}},
            {"$limit": 100}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 4: Collaborations
    # ========================================================================

    def q4_collaborations(self, actor_name: str):
        """Requête 4: Réalisateurs ayant travaillé avec un acteur"""
        pipeline = [
            {"$match": {"cast.name": {"$regex": actor_name, "$options": "i"}}},
            {"$unwind": "$directors"},
            {"$group": {
                "_id": "$directors.name",
                "nombre_films": {"$sum": 1}
            }},
            {"$sort": {"nombre_films": -1}},
            {"$project": {
                "_id": 0,
                "réalisateur": "$_id",
                "nombre_films": 1
            }},
            {"$limit": 50}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 5: Genres populaires
    # ========================================================================

    def q5_popular_genres(self, min_rating: float = 7.0, min_count: int = 50):
        """Requête 5: Genres populaires"""
        pipeline = [
            {"$unwind": "$genres"},
            {"$match": {"rating.average": {"$gt": min_rating}}},
            {"$group": {
                "_id": "$genres",
                "count": {"$sum": 1},
                "avg_rating": {"$avg": "$rating.average"}
            }},
            {"$match": {"count": {"$gt": min_count}}},
            {"$sort": {"avg_rating": -1}},
            {"$project": {
                "_id": 0,
                "genre": "$_id",
                "note_moyenne": "$avg_rating",
                "nombre_films": "$count"
            }}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 6: Évolution carrière
    # ========================================================================

    def q6_career_evolution(self, actor_name: str):
        """Requête 6: Évolution de carrière par décennie"""
        pipeline = [
            {"$match": {"cast.name": {"$regex": actor_name, "$options": "i"}}},
            {"$addFields": {"decade": {"$multiply": [{"$floor": {"$divide": ["$year", 10]}}, 10]}}},
            {"$match": {"decade": {"$gt": 0}}},
            {"$group": {
                "_id": "$decade",
                "nombre_films": {"$sum": 1},
                "note_moyenne": {"$avg": "$rating.average"}
            }},
            {"$sort": {"_id": 1}},
            {"$project": {
                "_id": 0,
                "décennie": {"$concat": [{"$toString": "$_id"}, "s"]},
                "nombre_films": 1,
                "note_moyenne": 1
            }}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 7: Classement par genre
    # ========================================================================

    def q7_top_films_per_genre(self, top_n: int = 3):
        """Requête 7: Top N films par genre"""
        pipeline = [
            {"$unwind": "$genres"},
            {"$sort": {"rating.average": -1}},
            {"$group": {
                "_id": "$genres",
                "films": {
                    "$push": {
                        "title": "$title",
                        "year": "$year",
                        "rating": "$rating.average"
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "genre": "$_id",
                "films": {"$slice": ["$films", top_n]}
            }},
            {"$limit": 50}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 8: Carrière propulsée
    # ========================================================================

    def q8_breakthrough_careers(self, threshold_votes: int = 200000):
        """Requête 8: Carrières propulsées"""
        pipeline = [
            {"$match": {"cast": {"$exists": True, "$ne": []}}},
            {"$unwind": "$cast"},
            {"$group": {
                "_id": "$cast.name",
                "total_films": {"$sum": 1},
                "blockbuster_films": {
                    "$sum": {"$cond": [{"$gt": ["$rating.votes", threshold_votes]}, 1, 0]}
                },
                "max_votes": {"$max": "$rating.votes"}
            }},
            {"$match": {
                "total_films": {"$gt": 5},
                "blockbuster_films": {"$gt": 0},
                "max_votes": {"$gt": threshold_votes}
            }},
            {"$sort": {"max_votes": -1}},
            {"$project": {
                "_id": 0,
                "acteur": "$_id",
                "films_total": "$total_films",
                "blockbuster_films": 1,
                "max_votes": 1
            }},
            {"$limit": 50}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 9: Réalisateurs prolifiques
    # ========================================================================

    def q9_prolific_directors(self, min_films: int = 10):
        """Requête 9: Réalisateurs prolifiques"""
        pipeline = [
            {"$unwind": "$directors"},
            {"$group": {
                "_id": "$directors.name",
                "nombre_films": {"$sum": 1},
                "note_moyenne": {"$avg": "$rating.average"},
                "note_min": {"$min": "$rating.average"},
                "note_max": {"$max": "$rating.average"}
            }},
            {"$match": {"nombre_films": {"$gte": min_films}}},
            {"$sort": {"nombre_films": -1}},
            {"$project": {
                "_id": 0,
                "réalisateur": "$_id",
                "nombre_films": 1,
                "note_moyenne": 1,
                "note_min": 1,
                "note_max": 1
            }},
            {"$limit": 30}
        ]
        return list(self.db.movies_complete.aggregate(pipeline))

    def close(self):
        """Fermer connexion"""
        self.client.close()


def print_header(title):
    """Afficher en-tête"""
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print(f"{'=' * 80}")


def print_results(results, limit=3):
    """Afficher résultats"""
    if not results:
        print("❌ Aucun résultat")
        return

    print(f"✅ {len(results)} résultat(s)\n")

    for i, doc in enumerate(results[:limit]):
        print(f"  {i + 1}. {doc}")

    if len(results) > limit:
        print(f"\n  ... et {len(results) - limit} autres")


def main():
    """Main"""

    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "🎬 T2.4: LES 9 REQUÊTES ULTRA-RAPIDES".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("║" + "(Sur movies_complete - PAS de $lookup!)".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝\n")

    print(f"⏰ Début: {datetime.now().strftime('%H:%M:%S')}\n")

    queries = MovieQueriesFast()
    times = {}

    try:
        # Q1
        print_header("1️⃣  FILMOGRAPHIE (Tom Hanks)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q1_actor_filmography("Tom Hanks")
        elapsed = time.time() - start
        times["q1"] = elapsed
        print_results(results, 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q2
        print_header("2️⃣  TOP 5 FILMS DRAMA (2020-2024)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q2_top_n_films("Drama", 2020, 2024, 5)
        elapsed = time.time() - start
        times["q2"] = elapsed
        print_results(results, 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q3
        print_header("3️⃣  ACTEURS MULTI-RÔLES")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q3_actors_multi_roles()
        elapsed = time.time() - start
        times["q3"] = elapsed
        print_results(results[:3], 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q4
        print_header("4️⃣  RÉALISATEURS AVEC TOM HANKS")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q4_collaborations("Tom Hanks")
        elapsed = time.time() - start
        times["q4"] = elapsed
        print_results(results, 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q5
        print_header("5️⃣  GENRES POPULAIRES (>7.0, >50 films)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q5_popular_genres()
        elapsed = time.time() - start
        times["q5"] = elapsed
        print_results(results, 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q6
        print_header("6️⃣  ÉVOLUTION CARRIÈRE (Tom Hanks)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q6_career_evolution("Tom Hanks")
        elapsed = time.time() - start
        times["q6"] = elapsed
        print_results(results, 10)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q7
        print_header("7️⃣  TOP 3 FILMS PAR GENRE")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q7_top_films_per_genre(3)
        elapsed = time.time() - start
        times["q7"] = elapsed
        print_results(results[:3], 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q8
        print_header("8️⃣  CARRIÈRES PROPULSÉES")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q8_breakthrough_careers()
        elapsed = time.time() - start
        times["q8"] = elapsed
        print_results(results, 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # Q9
        print_header("9️⃣  RÉALISATEURS PROLIFIQUES (>10 films)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.q9_prolific_directors()
        elapsed = time.time() - start
        times["q9"] = elapsed
        print_results(results, 3)
        print(f"⏱️  Temps: {elapsed:.3f}s")

        # STATS
        print_header("📊 STATISTIQUES FINALES")

        total = sum(times.values())

        print("\nTemps par requête:")
        for i in range(1, 10):
            key = f"q{i}"
            if key in times:
                print(f"  Requête {i}: {times[key]:>8.3f}s")

        print(f"\n  {'─' * 20}")
        print(f"  TOTAL:  {total:>8.3f}s ({total / 60:.2f} minutes)")
        print(f"  Moy:    {total / 9:>8.3f}s par requête\n")

        print("✅ COMPLÉTÉ AVEC SUCCÈS!\n")
        print(f"⏰ Fin: {datetime.now().strftime('%H:%M:%S')}\n")

    except Exception as e:
        print(f"\n❌ ERREUR: {e}\n")
        import traceback
        traceback.print_exc()

    finally:
        queries.close()


if __name__ == '__main__':
    main()