#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
T2.4 OPTIMISÉE - Construction RAPIDE de movies_complete
(Sans les find_one() répétés qui bloquent!)
"""

from pymongo import MongoClient
import time
from datetime import datetime


class MovieCompleteBuilderFast:
    """Construire movies_complete RAPIDEMENT"""

    def __init__(self):
        """Initialiser connexion"""
        print("📡 Connexion à MongoDB...")
        self.client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=60000)
        self.db = self.client["cineexplorer_flat"]
        print("✅ Connecté!\n")

    def build_movies_complete(self):
        """Construire movies_complete (VERSION RAPIDE)"""

        print("🎬 Construction de movies_complete (VERSION OPTIMISÉE)...")
        print("=" * 80)

        start_total = time.time()

        # Étape 1: Récupérer tous les films
        print("\n1️⃣  Récupération des films...")
        movies = list(self.db.movies.find({}, {"_id": 0}))
        print(f"   ✅ {len(movies):,} films trouvés")

        # Étape 2: Récupérer ratings
        print("\n2️⃣  Récupération des ratings...")
        ratings_dict = {}
        for rating in self.db.ratings.find({}, {"movie_id": 1, "average_rating": 1, "num_votes": 1}):
            ratings_dict[rating["movie_id"]] = {
                "average": rating["average_rating"],
                "votes": rating["num_votes"]
            }
        print(f"   ✅ {len(ratings_dict):,} ratings trouvés")

        # Étape 3: Récupérer genres
        print("\n3️⃣  Récupération des genres...")
        genres_dict = {}
        for genre in self.db.genres.find({}, {"movie_id": 1, "genre": 1}):
            if genre["movie_id"] not in genres_dict:
                genres_dict[genre["movie_id"]] = []
            genres_dict[genre["movie_id"]].append(genre["genre"])
        print(f"   ✅ {len(genres_dict):,} films avec genres trouvés")

        # Étape 4: Charger TOUS les persons EN MÉMOIRE (une seule fois)
        print("\n4️⃣  Chargement des persons en mémoire...")
        persons_dict = {}
        for person in self.db.persons.find({}, {"person_id": 1, "name": 1}):
            persons_dict[person["person_id"]] = person["name"]
        print(f"   ✅ {len(persons_dict):,} persons chargés")

        # Étape 5: Récupérer réalisateurs (RAPIDE - sans find_one!)
        print("\n5️⃣  Récupération des réalisateurs...")
        directors_dict = {}
        for director in self.db.directors.find({}, {"movie_id": 1, "person_id": 1}):
            if director["movie_id"] not in directors_dict:
                directors_dict[director["movie_id"]] = []

            person_name = persons_dict.get(director["person_id"], "Unknown")
            directors_dict[director["movie_id"]].append({
                "person_id": director["person_id"],
                "name": person_name
            })
        print(f"   ✅ {len(directors_dict):,} films avec réalisateurs trouvés")

        # Étape 6: Récupérer acteurs/cast (RAPIDE - sans find_one!)
        print("\n6️⃣  Récupération des acteurs...")
        cast_dict = {}

        # Étape 6a: Charger tous les principals
        for principal in self.db.principals.find({}, {"movie_id": 1, "person_id": 1}):
            if principal["movie_id"] not in cast_dict:
                cast_dict[principal["movie_id"]] = {}

            person_id = principal["person_id"]
            person_name = persons_dict.get(person_id, "Unknown")

            if person_id not in cast_dict[principal["movie_id"]]:
                cast_dict[principal["movie_id"]][person_id] = {
                    "person_id": person_id,
                    "name": person_name,
                    "characters": []
                }
        print(f"   • {len(cast_dict):,} films avec acteurs trouvés")

        # Étape 6b: Ajouter les personnages
        for character in self.db.characters.find({}, {"movie_id": 1, "person_id": 1, "name": 1}):
            if character["movie_id"] in cast_dict:
                if character["person_id"] in cast_dict[character["movie_id"]]:
                    cast_dict[character["movie_id"]][character["person_id"]]["characters"].append(character["name"])

        print(f"   ✅ Cast avec personnages complété")

        # Étape 7: Récupérer écrivains (RAPIDE - sans find_one!)
        print("\n7️⃣  Récupération des écrivains...")
        writers_dict = {}
        for writer in self.db.writers.find({}, {"movie_id": 1, "person_id": 1}):
            if writer["movie_id"] not in writers_dict:
                writers_dict[writer["movie_id"]] = []

            person_name = persons_dict.get(writer["person_id"], "Unknown")
            writers_dict[writer["movie_id"]].append({
                "person_id": writer["person_id"],
                "name": person_name
            })
        print(f"   ✅ {len(writers_dict):,} films avec écrivains trouvés")

        # Étape 8: Construire les documents complets
        print("\n8️⃣  Construction des documents complets...")
        complete_movies = []

        for movie in movies:
            movie_id = movie["movie_id"]

            # Convertir cast_dict en liste
            cast_list = list(cast_dict.get(movie_id, {}).values()) if movie_id in cast_dict else []

            complete_doc = {
                "_id": movie_id,
                "title": movie["primary_title"],
                "year": movie["start_year"],
                "runtime": movie.get("runtime_minutes"),
                "rating": ratings_dict.get(movie_id, {"average": None, "votes": 0}),
                "genres": genres_dict.get(movie_id, []),
                "directors": directors_dict.get(movie_id, []),
                "cast": cast_list,
                "writers": writers_dict.get(movie_id, [])
            }
            complete_movies.append(complete_doc)

        print(f"   ✅ {len(complete_movies):,} documents construits")

        # Étape 9: Insérer dans MongoDB
        print("\n9️⃣  Insertion dans MongoDB...")

        # Supprimer collection existante
        self.db.movies_complete.drop()
        print("   • Collection movies_complete supprimée (si existait)")

        # Insérer par batch
        batch_size = 1000
        for i in range(0, len(complete_movies), batch_size):
            batch = complete_movies[i:i + batch_size]
            self.db.movies_complete.insert_many(batch)
            if (i + batch_size) % 5000 == 0:
                print(f"   • Insertions: {min(i + batch_size, len(complete_movies))}/{len(complete_movies)}")

        print(f"   ✅ {len(complete_movies):,} documents insérés")

        # Étape 10: Créer des indexes
        print("\n🔟 Création des indexes...")
        self.db.movies_complete.create_index("title")
        self.db.movies_complete.create_index("year")
        self.db.movies_complete.create_index("genres")
        self.db.movies_complete.create_index("rating.average")
        print("   ✅ Indexes créés")

        # Temps total
        elapsed = time.time() - start_total

        print("\n" + "=" * 80)
        print(f"\n✅ COMPLÉTÉ AVEC SUCCÈS!")
        print(f"⏱️  Temps total: {elapsed:.2f} secondes ({elapsed / 60:.1f} minutes)")
        print(f"\n📊 Statistiques:")
        print(f"   • Films: {len(complete_movies):,}")
        print(f"   • Genres: {len(genres_dict):,} films avec genres")
        print(f"   • Réalisateurs: {len(directors_dict):,} films")
        print(f"   • Acteurs: {len(cast_dict):,} films")
        print(f"   • Écrivains: {len(writers_dict):,} films")

        # Exemple de document
        print(f"\n📋 Exemple de document movies_complete:")
        example = self.db.movies_complete.find_one({"rating.votes": {"$gt": 1000000}})
        if example:
            import json
            print(json.dumps({
                "_id": example["_id"],
                "title": example["title"],
                "year": example["year"],
                "rating": example["rating"],
                "genres": example["genres"][:3] if example["genres"] else [],
                "directors_count": len(example.get("directors", [])),
                "cast_count": len(example.get("cast", [])),
                "writers_count": len(example.get("writers", []))
            }, indent=2, ensure_ascii=False))

        print("\n" + "=" * 80 + "\n")

    def close(self):
        """Fermer connexion"""
        self.client.close()


def main():
    """Main"""

    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "🎬 T2.4: CONSTRUCTION DE MOVIES_COMPLETE (OPTIMISÉE)".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝\n")

    print(f"⏰ Début: {datetime.now().strftime('%H:%M:%S')}\n")

    builder = MovieCompleteBuilderFast()

    try:
        builder.build_movies_complete()
        print(f"⏰ Fin: {datetime.now().strftime('%H:%M:%S')}\n")

    except Exception as e:
        print(f"\n❌ ERREUR: {e}\n")
        import traceback
        traceback.print_exc()

    finally:
        builder.close()


if __name__ == '__main__':
    main()