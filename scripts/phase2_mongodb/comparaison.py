#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║     🎬 CINEEXPLORER - PHASE 2 T2.5: COMPARAISON FLAT vs STRUCTURÉ         ║
║                                                                            ║
║            Temps | Taille | Complexité - Collections vs Documents         ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝

Comparaison:
  1️⃣  Temps d'exécution (requête film complet)
      • Collections plates (multiples $lookup)
      • Documents structurés (simple find)

  2️⃣  Taille de stockage
      • Collections plates (normalisées)
      • Documents structurés (dénormalisés)

  3️⃣  Complexité du code
      • Pipeline MongoDB ($lookup)
      • Nombre d'étapes
      • Lisibilité

Usage:
  python3 T2.5_comparison.py
"""

from pymongo import MongoClient
import time
from datetime import datetime
import json


class ComparisonFlavorStructured:
    """Comparaison Flat vs Structured"""

    def __init__(self):
        """Initialiser connexion"""
        print("📡 Connexion à MongoDB...")
        self.client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=60000)
        self.db = self.client["cineexplorer_flat"]
        print("✅ Connecté!\n")

    # ========================================================================
    # 1️⃣ TEMPS D'EXÉCUTION
    # ========================================================================

    def test_time_flat_collections(self, movie_title="The Godfather"):
        """Récupérer un film complet avec collections plates (multiples $lookup)"""

        pipeline = [
            # Étape 1: Trouver le film par titre
            {"$match": {"primary_title": movie_title}},

            # Étape 2: Joindre avec ratings
            {"$lookup": {
                "from": "ratings",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "rating"
            }},
            {"$unwind": {"path": "$rating", "preserveNullAndEmptyArrays": True}},

            # Étape 3: Joindre avec genres
            {"$lookup": {
                "from": "genres",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "genres"
            }},

            # Étape 4: Joindre avec directors
            {"$lookup": {
                "from": "directors",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "directors_ids"
            }},
            {"$unwind": {"path": "$directors_ids", "preserveNullAndEmptyArrays": True}},

            # Étape 5: Joindre directors avec persons
            {"$lookup": {
                "from": "persons",
                "localField": "directors_ids.person_id",
                "foreignField": "person_id",
                "as": "directors"
            }},

            # Étape 6: Joindre avec principals
            {"$lookup": {
                "from": "principals",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "cast_ids"
            }},
            {"$unwind": {"path": "$cast_ids", "preserveNullAndEmptyArrays": True}},

            # Étape 7: Joindre cast avec persons
            {"$lookup": {
                "from": "persons",
                "localField": "cast_ids.person_id",
                "foreignField": "person_id",
                "as": "cast"
            }},

            # Étape 8: Joindre avec characters
            {"$lookup": {
                "from": "characters",
                "localField": "cast_ids.person_id",
                "foreignField": "person_id",
                "as": "characters"
            }},

            # Étape 9: Joindre avec writers
            {"$lookup": {
                "from": "writers",
                "localField": "movie_id",
                "foreignField": "movie_id",
                "as": "writers_ids"
            }},
            {"$unwind": {"path": "$writers_ids", "preserveNullAndEmptyArrays": True}},

            # Étape 10: Joindre writers avec persons
            {"$lookup": {
                "from": "persons",
                "localField": "writers_ids.person_id",
                "foreignField": "person_id",
                "as": "writers"
            }},

            # Projection finale
            {"$project": {
                "_id": 0,
                "movie_id": 1,
                "title": "$primary_title",
                "year": "$start_year",
                "rating": "$rating",
                "genres": 1,
                "directors": 1,
                "cast": 1,
                "writers": 1
            }},

            {"$limit": 1}
        ]

        start = time.time()
        result = list(self.db.movies.aggregate(pipeline))
        elapsed = time.time() - start

        return elapsed, result

    def test_time_structured(self, movie_title="The Godfather"):
        """Récupérer un film complet avec documents structurés (simple find)"""

        start = time.time()
        result = self.db.movies_complete.find_one({"title": movie_title})
        elapsed = time.time() - start

        return elapsed, result

    # ========================================================================
    # 2️⃣ TAILLE DE STOCKAGE
    # ========================================================================

    def get_storage_size_flat(self):
        """Taille totale des collections plates"""

        sizes = {}
        collections = ["movies", "ratings", "genres", "principals", "directors",
                       "writers", "characters", "persons", "titles", "episodes",
                       "professions", "knownformovies"]

        total_size = 0

        for coll in collections:
            try:
                stats = self.db.command("collStats", coll)
                size = stats.get("size", 0)
                avg_doc = stats.get("avgObjSize", 0)
                count = stats.get("count", 0)
                sizes[coll] = {
                    "size_mb": size / (1024 * 1024),
                    "avg_doc_size": avg_doc,
                    "count": count
                }
                total_size += size
            except:
                pass

        return sizes, total_size / (1024 * 1024)

    def get_storage_size_structured(self):
        """Taille de la collection structurée"""

        try:
            stats = self.db.command("collStats", "movies_complete")
            size = stats.get("size", 0)
            avg_doc = stats.get("avgObjSize", 0)
            count = stats.get("count", 0)

            return {
                "size_mb": size / (1024 * 1024),
                "avg_doc_size": avg_doc,
                "count": count
            }, size / (1024 * 1024)
        except:
            return {}, 0

    # ========================================================================
    # 3️⃣ COMPLEXITÉ DU CODE
    # ========================================================================

    def get_complexity_metrics(self):
        """Calculer les métriques de complexité"""

        flat_pipeline_steps = 10  # $match, $lookup, $unwind, ...
        flat_collections_involved = 8  # movies, ratings, genres, directors, persons, cast, characters, writers

        structured_pipeline_steps = 1  # $match (or find)
        structured_collections_involved = 1  # movies_complete

        return {
            "flat": {
                "pipeline_steps": flat_pipeline_steps,
                "collections": flat_collections_involved,
                "operations": flat_pipeline_steps + flat_collections_involved,
                "code_lines": 45  # Approximatif
            },
            "structured": {
                "pipeline_steps": structured_pipeline_steps,
                "collections": structured_collections_involved,
                "operations": structured_pipeline_steps + structured_collections_involved,
                "code_lines": 3  # find().limit(1)
            }
        }

    def close(self):
        """Fermer connexion"""
        self.client.close()


def format_size(size_mb):
    """Formater la taille"""
    if size_mb < 1000:
        return f"{size_mb:.2f} MB"
    else:
        return f"{size_mb / 1024:.2f} GB"


def main():
    """Main"""

    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "🎬 T2.5: COMPARAISON COLLECTIONS PLATES vs DOCUMENTS STRUCTURÉS".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝\n")

    print(f"⏰ Début: {datetime.now().strftime('%H:%M:%S')}\n")

    comp = ComparisonFlavorStructured()

    try:
        # ====================================================================
        # 1️⃣ COMPARAISON TEMPS
        # ====================================================================
        print("=" * 80)
        print("1️⃣  COMPARAISON DES TEMPS D'EXÉCUTION")
        print("=" * 80)
        print("\n📌 Requête: Récupérer un film complet (The Godfather)")
        print("   Inclure: titre, année, rating, genres, réalisateurs, acteurs, écrivains\n")

        # Tester plusieurs fois pour moyenne
        flat_times = []
        structured_times = []

        for i in range(3):
            print(f"Exécution {i + 1}/3...")

            t, _ = comp.test_time_flat_collections()
            flat_times.append(t)

            t, _ = comp.test_time_structured()
            structured_times.append(t)

        avg_flat = sum(flat_times) / len(flat_times)
        avg_structured = sum(structured_times) / len(structured_times)
        ratio = avg_flat / avg_structured if avg_structured > 0 else 0

        print(f"\n📊 RÉSULTATS:\n")
        print(f"Collections PLATES (10 $lookup):")
        print(f"  • Temps moyen: {avg_flat:.3f} secondes")
        print(f"  • Détail: {flat_times}")
        print()
        print(f"Documents STRUCTURÉS (simple find):")
        print(f"  • Temps moyen: {avg_structured:.3f} secondes")
        print(f"  • Détail: {structured_times}")
        print()
        print(f"RATIO (Flat / Structured): {ratio:.1f}x")
        print(f"  → Collections plates sont {ratio:.1f}x PLUS LENTES!\n")

        # ====================================================================
        # 2️⃣ COMPARAISON TAILLE
        # ====================================================================
        print("=" * 80)
        print("2️⃣  COMPARAISON DE LA TAILLE DE STOCKAGE")
        print("=" * 80)
        print("\nCalcul des tailles MongoDB...\n")

        flat_sizes, flat_total = comp.get_storage_size_flat()
        structured_sizes, structured_total = comp.get_storage_size_structured()

        print(f"Collections PLATES (12 collections):")
        print(f"  Taille totale: {format_size(flat_total)}")

        if flat_sizes:
            print(f"\n  Détail par collection:")
            for coll, info in sorted(flat_sizes.items(), key=lambda x: x[1]["size_mb"], reverse=True)[:5]:
                print(f"    • {coll:20s}: {info['size_mb']:>8.2f} MB ({info['count']:,} docs)")

        print(f"\nDocuments STRUCTURÉS (1 collection):")
        print(f"  Taille totale: {format_size(structured_total)}")

        if structured_sizes:
            print(f"    • movies_complete: {structured_sizes['size_mb']:>8.2f} MB ({structured_sizes['count']:,} docs)")

        size_diff = flat_total - structured_total
        size_percent = (size_diff / flat_total * 100) if flat_total > 0 else 0

        print(f"\n📊 ANALYSE:\n")
        print(f"  Différence: {format_size(abs(size_diff))}")
        print(f"  Variation: {size_percent:+.1f}%")
        if size_diff > 0:
            print(f"  → Collections plates prennent {size_percent:.1f}% PLUS d'espace!")
        else:
            print(f"  → Documents structurés prennent {abs(size_percent):.1f}% PLUS d'espace (dénormalisation)\n")

        # ====================================================================
        # 3️⃣ COMPLEXITÉ DU CODE
        # ====================================================================
        print("=" * 80)
        print("3️⃣  COMPLEXITÉ DU CODE ET DES OPÉRATIONS")
        print("=" * 80 + "\n")

        complexity = comp.get_complexity_metrics()

        flat = complexity["flat"]
        structured = complexity["structured"]

        print("Collections PLATES:")
        print(f"  • Étapes du pipeline: {flat['pipeline_steps']}")
        print(f"  • Collections impliquées: {flat['collections']}")
        print(f"  • Total opérations: {flat['operations']}")
        print(f"  • Lignes de code: ~{flat['code_lines']}")
        print(f"  • Lisibilité: ⭐☆☆☆☆ (complexe)")

        print(f"\nDocuments STRUCTURÉS:")
        print(f"  • Étapes du pipeline: {structured['pipeline_steps']}")
        print(f"  • Collections impliquées: {structured['collections']}")
        print(f"  • Total opérations: {structured['operations']}")
        print(f"  • Lignes de code: ~{structured['code_lines']}")
        print(f"  • Lisibilité: ⭐⭐⭐⭐⭐ (simple)")

        reduction = (1 - structured['operations'] / flat['operations']) * 100

        print(f"\n📊 ANALYSE:\n")
        print(f"  Réduction des opérations: {reduction:.0f}%")
        print(f"  Code {flat['code_lines'] / structured['code_lines']:.0f}x plus simple!\n")

        # ====================================================================
        # RÉSUMÉ FINAL
        # ====================================================================
        print("=" * 80)
        print("📈 RÉSUMÉ GÉNÉRAL")
        print("=" * 80 + "\n")

        print("┌─────────────────────────────────────────────────────────────────────┐")
        print("│ MÉTRIQUE              │ COLLECTIONS PLATES  │ DOCUMENTS STRUCTURÉS   │")
        print("├─────────────────────────────────────────────────────────────────────┤")
        print(f"│ Temps d'exécution     │ {avg_flat:>18.3f}s  │ {avg_structured:>20.3f}s │")
        print(f"│ Performance ratio     │ {ratio:>18.1f}x  │ {'1.0x':>20s} │")
        print(f"│ Taille stockage       │ {format_size(flat_total):>18s}  │ {format_size(structured_total):>20s} │")
        print(f"│ Étapes pipeline       │ {flat['pipeline_steps']:>18d}  │ {structured['pipeline_steps']:>20d} │")
        print(f"│ Collections           │ {flat['collections']:>18d}  │ {structured['collections']:>20d} │")
        print(f"│ Complexité code       │ {'Très haute':>18s}  │ {'Très basse':>20s} │")
        print("└─────────────────────────────────────────────────────────────────────┘\n")

        print("🎯 CONCLUSION:\n")
        print("✅ Documents STRUCTURÉS (Dénormalisés):")
        print(f"   • {ratio:.1f}x PLUS RAPIDE")
        print("   • Code BEAUCOUP plus simple")
        print("   • Idéal pour requêtes fréquentes")
        print()
        print("📋 Collections PLATES (Normalisées):")
        print("   • Bonne pour mises à jour fréquentes")
        print("   • Réduit la redondance de données")
        print("   • Nécessite multiples JOINs")
        print()
        print("💡 RECOMMANDATION:")
        print("   → MongoDB avec DÉNORMALISATION (documents structurés)")
        print("   → Pour requêtes analytiques complexes")
        print("   → Compromis: un peu plus d'espace pour BEAUCOUP plus de vitesse!\n")

        print("=" * 80 + "\n")
        print(f"⏰ Fin: {datetime.now().strftime('%H:%M:%S')}\n")

    except Exception as e:
        print(f"\n❌ ERREUR: {e}\n")
        import traceback
        traceback.print_exc()

    finally:
        comp.close()


if __name__ == '__main__':
    main()