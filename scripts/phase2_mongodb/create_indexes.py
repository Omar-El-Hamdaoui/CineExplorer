#!/usr/bin/env python3
"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - Création des Index MongoDB                              ║
║                                                                                ║
║     Script à exécuter une fois après l'import des données                     ║
║     Usage: python scripts/phase2_mongodb/create_indexes.py                    ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
import sys

# Configuration
MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
DATABASE_NAME = "cineexplorer_flat"


def create_indexes():
    """Crée tous les index nécessaires pour optimiser les performances"""
    
    print("=" * 60)
    print("  CINEEXPLORER - Création des Index MongoDB")
    print("=" * 60)
    print()
    
    try:
        # Connexion
        print("📡 Connexion à MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DATABASE_NAME]
        print("✅ Connecté au Replica Set")
        print()
        
        # =====================================================================
        # Index pour movies_complete
        # =====================================================================
        print("📦 Collection: movies_complete")
        print("-" * 40)
        
        indexes_movies = [
            {"keys": [("genres", ASCENDING)], "name": "idx_genres"},
            {"keys": [("year", ASCENDING)], "name": "idx_year"},
            {"keys": [("rating.average", DESCENDING)], "name": "idx_rating_avg"},
            {"keys": [("rating.votes", DESCENDING)], "name": "idx_rating_votes"},
            {"keys": [("directors.name", ASCENDING)], "name": "idx_directors_name"},
            {"keys": [("cast.name", ASCENDING)], "name": "idx_cast_name"},
            {"keys": [("title", TEXT)], "name": "idx_title_text"},
        ]
        
        for idx in indexes_movies:
            try:
                db.movies_complete.create_index(idx["keys"], name=idx["name"])
                print(f"   ✅ {idx['name']}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"   ⏭️  {idx['name']} (existe déjà)")
                else:
                    print(f"   ❌ {idx['name']}: {e}")
        
        print()
        
        # =====================================================================
        # Index pour persons
        # =====================================================================
        print("📦 Collection: persons")
        print("-" * 40)
        
        indexes_persons = [
            {"keys": [("name", TEXT)], "name": "idx_name_text"},
            {"keys": [("birth_year", ASCENDING)], "name": "idx_birth_year"},
        ]
        
        for idx in indexes_persons:
            try:
                db.persons.create_index(idx["keys"], name=idx["name"])
                print(f"   ✅ {idx['name']}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"   ⏭️  {idx['name']} (existe déjà)")
                else:
                    print(f"   ❌ {idx['name']}: {e}")
        
        print()
        
        # =====================================================================
        # Index pour movies (collection plate)
        # =====================================================================
        print("📦 Collection: movies")
        print("-" * 40)
        
        indexes_movies_flat = [
            {"keys": [("primary_title", ASCENDING)], "name": "idx_primary_title"},
            {"keys": [("start_year", ASCENDING)], "name": "idx_start_year"},
        ]
        
        for idx in indexes_movies_flat:
            try:
                db.movies.create_index(idx["keys"], name=idx["name"])
                print(f"   ✅ {idx['name']}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"   ⏭️  {idx['name']} (existe déjà)")
                else:
                    print(f"   ❌ {idx['name']}: {e}")
        
        print()
        
        # =====================================================================
        # Résumé
        # =====================================================================
        print("=" * 60)
        print("  RÉSUMÉ DES INDEX")
        print("=" * 60)
        
        for collection_name in ['movies_complete', 'persons', 'movies']:
            collection = db[collection_name]
            indexes = list(collection.list_indexes())
            print(f"\n📦 {collection_name}: {len(indexes)} index")
            for idx in indexes:
                print(f"   - {idx['name']}: {list(idx['key'].keys())}")
        
        print()
        print("✅ Création des index terminée!")
        print()
        print("💡 Les index sont permanents et survivent aux redémarrages.")
        print("   Ils accélèrent significativement les requêtes de filtrage,")
        print("   tri et agrégation.")
        print()
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False


def show_indexes():
    """Affiche les index existants"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DATABASE_NAME]
        
        print("Index existants:")
        print("-" * 40)
        
        for collection_name in ['movies_complete', 'persons', 'movies']:
            collection = db[collection_name]
            indexes = list(collection.list_indexes())
            print(f"\n{collection_name}:")
            for idx in indexes:
                print(f"  - {idx['name']}")
        
        client.close()
        
    except Exception as e:
        print(f"Erreur: {e}")


def drop_indexes():
    """Supprime tous les index (sauf _id)"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[DATABASE_NAME]
        
        print("Suppression des index...")
        
        for collection_name in ['movies_complete', 'persons', 'movies']:
            db[collection_name].drop_indexes()
            print(f"  ✅ {collection_name}")
        
        print("Index supprimés (sauf _id)")
        client.close()
        
    except Exception as e:
        print(f"Erreur: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--show":
            show_indexes()
        elif sys.argv[1] == "--drop":
            drop_indexes()
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python create_indexes.py        Créer les index")
            print("  python create_indexes.py --show Afficher les index")
            print("  python create_indexes.py --drop Supprimer les index")
        else:
            create_indexes()
    else:
        create_indexes()
