#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║     🎬 CINEEXPLORER - PHASE 2 T2.3: Les 9 Requêtes MongoDB               ║
║                                                                            ║
║            Exécution complète avec timeline et progrès                     ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝

Utilité:
  Exécuter les 9 requêtes MongoDB équivalentes aux requêtes SQL
  Afficher les résultats et les temps
  Comparer les performances

Usage:
  python3 T2.3_queries_complete.py

Temps estimé: 5-10 minutes (première exécution)
"""

from pymongo import MongoClient
import time
import traceback
from datetime import datetime


class MongoQueries:
    """Requêtes MongoDB optimisées"""

    def __init__(self):
        """Initialiser connexion"""
        print("📡 Connexion à MongoDB...")
        self.client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=120000)
        self.db = self.client["cineexplorer_flat"]
        self.stats = {}
        print("✅ Connecté!\n")

    # ========================================================================
    # REQUÊTE 1: Filmographie d'un acteur
    # ========================================================================

    def query_1_actor_filmography(self, actor_name: str):
        """Requête 1: Filmographie d'un acteur"""
        pipeline = [
            {"$match": {"name": {"$regex": actor_name, "$options": "i"}}},
            {"$lookup": {"from": "principals", "localField": "person_id", "foreignField": "person_id",
                         "as": "principal_data"}},
            {"$unwind": "$principal_data"},
            {"$lookup": {"from": "movies", "localField": "principal_data.movie_id", "foreignField": "movie_id",
                         "as": "movie_data"}},
            {"$unwind": "$movie_data"},
            {"$lookup": {"from": "genres", "localField": "principal_data.movie_id", "foreignField": "movie_id",
                         "as": "genre_data"}},
            {"$lookup": {"from": "ratings", "localField": "principal_data.movie_id", "foreignField": "movie_id",
                         "as": "rating_data"}},
            {"$group": {
                "_id": "$principal_data.movie_id",
                "titre": {"$first": "$movie_data.primary_title"},
                "année": {"$first": "$movie_data.start_year"},
                "genres": {"$push": "$genre_data.genre"},
                "note": {"$first": {"$arrayElemAt": ["$rating_data.average_rating", 0]}}
            }},
            {"$sort": {"année": -1}},
            {"$limit": 100}
        ]

        return list(self.db.persons.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 2: Top N films par genre
    # ========================================================================

    def query_2_top_n_films(self, genre: str, year_start: int, year_end: int, n: int = 10):
        """Requête 2: Top N films par genre et période"""
        pipeline = [
            {"$match": {"genre": genre}},
            {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "movie"}},
            {"$unwind": "$movie"},
            {"$match": {"movie.start_year": {"$gte": year_start, "$lte": year_end}}},
            {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "rating"}},
            {"$unwind": "$rating"},
            {"$match": {"rating.num_votes": {"$gt": 0}}},
            {"$sort": {"rating.average_rating": -1}},
            {"$project": {"_id": 0, "titre": "$movie.primary_title", "année": "$movie.start_year",
                          "note": "$rating.average_rating", "votes": "$rating.num_votes"}},
            {"$limit": n}
        ]

        return list(self.db.genres.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 3: Acteurs multi-rôles
    # ========================================================================

    def query_3_multi_role_actors(self):
        """Requête 3: Acteurs ayant joué plusieurs personnages"""
        pipeline = [
            {"$group": {"_id": {"person_id": "$person_id", "movie_id": "$movie_id"}, "nombre_roles": {"$sum": 1}}},
            {"$match": {"nombre_roles": {"$gt": 1}}},
            {"$lookup": {"from": "persons", "localField": "_id.person_id", "foreignField": "person_id",
                         "as": "person"}},
            {"$unwind": "$person"},
            {"$lookup": {"from": "movies", "localField": "_id.movie_id", "foreignField": "movie_id", "as": "movie"}},
            {"$unwind": "$movie"},
            {"$project": {"_id": 0, "acteur": "$person.name", "film": "$movie.primary_title", "nombre_roles": 1}},
            {"$sort": {"nombre_roles": -1}},
            {"$limit": 100}
        ]

        return list(self.db.characters.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 4: Collaborations
    # ========================================================================

    def query_4_collaborations(self, actor_name: str):
        """Requête 4: Réalisateurs ayant travaillé avec un acteur"""
        pipeline = [
            {"$match": {"name": {"$regex": actor_name, "$options": "i"}}},
            {"$lookup": {"from": "principals", "localField": "person_id", "foreignField": "person_id", "as": "films"}},
            {"$unwind": "$films"},
            {"$lookup": {"from": "directors", "localField": "films.movie_id", "foreignField": "movie_id",
                         "as": "directors"}},
            {"$unwind": "$directors"},
            {"$lookup": {"from": "persons", "localField": "directors.person_id", "foreignField": "person_id",
                         "as": "director_person"}},
            {"$unwind": "$director_person"},
            {"$group": {"_id": "$director_person.person_id", "réalisateur": {"$first": "$director_person.name"},
                        "nombre_films": {"$sum": 1}}},
            {"$sort": {"nombre_films": -1}},
            {"$project": {"_id": 0, "réalisateur": 1, "nombre_films": 1}},
            {"$limit": 50}
        ]

        return list(self.db.persons.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 5: Genres populaires
    # ========================================================================

    def query_5_popular_genres(self, min_rating: float = 7.0, min_count: int = 50):
        """Requête 5: Genres populaires"""
        pipeline = [
            {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "rating"}},
            {"$unwind": "$rating"},
            {"$group": {"_id": "$genre", "note_moyenne": {"$avg": "$rating.average_rating"},
                        "nombre_films": {"$sum": 1}}},
            {"$match": {"note_moyenne": {"$gt": min_rating}, "nombre_films": {"$gt": min_count}}},
            {"$sort": {"note_moyenne": -1}},
            {"$project": {"_id": 0, "genre": "$_id", "note_moyenne": 1, "nombre_films": 1}}
        ]

        return list(self.db.genres.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 6: Évolution carrière
    # ========================================================================

    def query_6_career_evolution(self, actor_name: str):
        """Requête 6: Évolution de carrière par décennie"""
        pipeline = [
            {"$match": {"name": {"$regex": actor_name, "$options": "i"}}},
            {"$lookup": {"from": "principals", "localField": "person_id", "foreignField": "person_id", "as": "films"}},
            {"$unwind": "$films"},
            {"$lookup": {"from": "movies", "localField": "films.movie_id", "foreignField": "movie_id", "as": "movie"}},
            {"$unwind": "$movie"},
            {"$lookup": {"from": "ratings", "localField": "films.movie_id", "foreignField": "movie_id",
                         "as": "rating"}},
            {"$unwind": {"path": "$rating", "preserveNullAndEmptyArrays": True}},
            {"$addFields": {"décennie": {"$multiply": [{"$floor": {"$divide": ["$movie.start_year", 10]}}, 10]}}},
            {"$match": {"décennie": {"$gt": 0}}},
            {"$group": {"_id": "$décennie", "nombre_films": {"$sum": 1},
                        "note_moyenne": {"$avg": "$rating.average_rating"}}},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "décennie": {"$concat": [{"$toString": "$_id"}, "s"]}, "nombre_films": 1,
                          "note_moyenne": 1}}
        ]

        return list(self.db.persons.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 7: Classement par genre
    # ========================================================================

    def query_7_top_films_per_genre(self, top_n: int = 3):
        """Requête 7: Top N films par genre"""
        pipeline = [
            {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "movie"}},
            {"$unwind": "$movie"},
            {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "rating"}},
            {"$unwind": "$rating"},
            {"$group": {"_id": "$genre", "films": {
                "$push": {"titre": "$movie.primary_title", "année": "$movie.start_year",
                          "note": "$rating.average_rating"}}}},
            {"$project": {"_id": 0, "genre": "$_id", "films": {"$slice": ["$films", top_n]}}},
            {"$limit": 50}
        ]

        return list(self.db.genres.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 8: Carrière propulsée
    # ========================================================================

    def query_8_breakthrough_careers(self, threshold_votes: int = 200000):
        """Requête 8: Carrières propulsées par un blockbuster"""
        pipeline = [
            {"$lookup": {"from": "principals", "localField": "person_id", "foreignField": "person_id", "as": "films"}},
            {"$unwind": "$films"},
            {"$lookup": {"from": "ratings", "localField": "films.movie_id", "foreignField": "movie_id",
                         "as": "rating"}},
            {"$unwind": "$rating"},
            {"$group": {
                "_id": "$person_id",
                "acteur": {"$first": "$name"},
                "films_avant": {"$sum": {"$cond": [{"$lte": ["$rating.num_votes", threshold_votes]}, 1, 0]}},
                "films_après": {"$sum": {"$cond": [{"$gt": ["$rating.num_votes", threshold_votes]}, 1, 0]}},
                "max_votes": {"$max": "$rating.num_votes"},
                "max_note": {
                    "$max": {"$cond": [{"$gt": ["$rating.num_votes", threshold_votes]}, "$rating.average_rating", 0]}}
            }},
            {"$match": {"films_avant": {"$gt": 0}, "films_après": {"$gt": 0}, "max_votes": {"$gt": threshold_votes}}},
            {"$sort": {"max_votes": -1}},
            {"$limit": 50}
        ]

        return list(self.db.persons.aggregate(pipeline))

    # ========================================================================
    # REQUÊTE 9: Réalisateurs prolifiques
    # ========================================================================

    def query_9_prolific_directors(self, min_films: int = 10):
        """Requête 9: Réalisateurs les plus prolifiques"""
        pipeline = [
            {"$lookup": {"from": "movies", "localField": "movie_id", "foreignField": "movie_id", "as": "movie"}},
            {"$unwind": "$movie"},
            {"$lookup": {"from": "persons", "localField": "person_id", "foreignField": "person_id", "as": "person"}},
            {"$unwind": "$person"},
            {"$lookup": {"from": "ratings", "localField": "movie_id", "foreignField": "movie_id", "as": "rating"}},
            {"$unwind": "$rating"},
            {"$lookup": {"from": "genres", "localField": "movie_id", "foreignField": "movie_id", "as": "genres"}},
            {"$group": {
                "_id": "$person_id",
                "réalisateur": {"$first": "$person.name"},
                "nombre_films": {"$sum": 1},
                "note_moyenne": {"$avg": "$rating.average_rating"},
                "note_min": {"$min": "$rating.average_rating"},
                "note_max": {"$max": "$rating.average_rating"},
                "genres_explorés": {"$sum": {"$size": "$genres"}}
            }},
            {"$match": {"nombre_films": {"$gte": min_films}}},
            {"$sort": {"nombre_films": -1}},
            {"$project": {"_id": 0, "réalisateur": 1, "nombre_films": 1, "note_moyenne": 1, "note_min": 1,
                          "note_max": 1, "genres_explorés": 1}},
            {"$limit": 30}
        ]

        return list(self.db.directors.aggregate(pipeline))

    def close(self):
        """Fermer connexion"""
        self.client.close()


def print_header(title):
    """Afficher un en-tête"""
    print(f"\n{'=' * 80}")
    print(f"{title}")
    print(f"{'=' * 80}")


def print_results(results, limit=5):
    """Afficher les résultats"""
    if not results:
        print("❌ Aucun résultat")
        return

    print(f"✅ {len(results)} résultat(s) trouvé(s)\n")

    for i, doc in enumerate(results[:limit]):
        print(f"  {i + 1}. {doc}")

    if len(results) > limit:
        print(f"\n  ... et {len(results) - limit} autres résultats")


def main():
    """Exécuter toutes les requêtes"""

    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "🎬 CINEEXPLORER - T2.3: LES 9 REQUÊTES MONGODB".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝\n")

    print(f"⏰ Début: {datetime.now().strftime('%H:%M:%S')}\n")

    queries = MongoQueries()
    times = {}

    try:
        # ====================================================================
        # REQUÊTE 1
        # ====================================================================
        print_header("1️⃣  FILMOGRAPHIE (Tom Hanks)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_1_actor_filmography("Tom Hanks")
        elapsed = time.time() - start
        times["query_1"] = elapsed
        print_results(results, 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 2
        # ====================================================================
        print_header("2️⃣  TOP 5 FILMS DRAMA (2020-2024)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_2_top_n_films("Drama", 2020, 2024, 5)
        elapsed = time.time() - start
        times["query_2"] = elapsed
        print_results(results, 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 3
        # ====================================================================
        print_header("3️⃣  ACTEURS MULTI-RÔLES")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_3_multi_role_actors()
        elapsed = time.time() - start
        times["query_3"] = elapsed
        print_results(results[:5], 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 4
        # ====================================================================
        print_header("4️⃣  RÉALISATEURS AVEC TOM HANKS")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_4_collaborations("Tom Hanks")
        elapsed = time.time() - start
        times["query_4"] = elapsed
        print_results(results, 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 5
        # ====================================================================
        print_header("5️⃣  GENRES POPULAIRES (>7.0, >50 films)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_5_popular_genres()
        elapsed = time.time() - start
        times["query_5"] = elapsed
        print_results(results, 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 6
        # ====================================================================
        print_header("6️⃣  ÉVOLUTION CARRIÈRE (Tom Hanks)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_6_career_evolution("Tom Hanks")
        elapsed = time.time() - start
        times["query_6"] = elapsed
        print_results(results, 10)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 7
        # ====================================================================
        print_header("7️⃣  TOP 3 FILMS PAR GENRE")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_7_top_films_per_genre(3)
        elapsed = time.time() - start
        times["query_7"] = elapsed
        print_results(results[:3], 3)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 8
        # ====================================================================
        print_header("8️⃣  CARRIÈRES PROPULSÉES (avant <200k, après >200k)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_8_breakthrough_careers()
        elapsed = time.time() - start
        times["query_8"] = elapsed
        print_results(results, 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # REQUÊTE 9
        # ====================================================================
        print_header("9️⃣  RÉALISATEURS PROLIFIQUES (>10 films)")
        start = time.time()
        print("⏳ Exécution...")
        results = queries.query_9_prolific_directors()
        elapsed = time.time() - start
        times["query_9"] = elapsed
        print_results(results, 5)
        print(f"⏱️  Temps: {elapsed:.2f} secondes")

        # ====================================================================
        # STATISTIQUES
        # ====================================================================
        print_header("📊 STATISTIQUES FINALES")

        total_time = sum(times.values())

        print("\nTemps par requête:")
        for i in range(1, 10):
            key = f"query_{i}"
            if key in times:
                print(f"  Requête {i}: {times[key]:>7.2f}s")

        print(f"\n  {'─' * 20}")
        print(f"  TOTAL:   {total_time:>7.2f}s ({total_time / 60:.1f} minutes)")

        avg_time = total_time / 9
        print(f"  Moyenne: {avg_time:>7.2f}s par requête\n")

        print("✅ COMPLÉTÉ AVEC SUCCÈS!\n")
        print(f"⏰ Fin: {datetime.now().strftime('%H:%M:%S')}\n")

    except Exception as e:
        print(f"\n❌ ERREUR: {e}\n")
        print("Détails:")
        traceback.print_exc()

    finally:
        queries.close()
        print("✅ Connexion fermée\n")


if __name__ == '__main__':
    main()