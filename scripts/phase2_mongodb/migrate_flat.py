#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║     🎬 CINEEXPLORER - PHASE 2 T2.2: Migration Collections Plates          ║
║                                                                            ║
║                    Migration SQLite → MongoDB                             ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝

Version: CORRIGÉE avec auto-détection du chemin imdb.db

Usage:
  python3 T2.2_migrate_flat_FIXED.py
"""

import sqlite3
import time
import os
from pathlib import Path
from typing import Dict, List, Tuple
from datetime import datetime

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure

    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration pour la migration"""

    # Chemins - AUTO-DETECT
    POSSIBLE_PATHS = [
        "/data/imdb.db",  # Docker path ✅ CELUI-LÀ!
        "./data/imdb.db",
        "../../../data/imdb.db",
        "/home/omar/Bureau/4A/Bases_de_Données_Avancées/cineexplorer/data/imdb.db",
    ]

    # Trouver le premier chemin qui existe
    DB_SQLITE = None
    for path in POSSIBLE_PATHS:
        if os.path.exists(path):
            DB_SQLITE = path
            print(f"✅ Trouvé: {path}")
            break

    if DB_SQLITE is None:
        print(f"⚠️  imdb.db non trouvé, essayera: {POSSIBLE_PATHS[0]}")
        DB_SQLITE = "/data/imdb.db"

    # MongoDB
    MONGO_URL = "mongodb://localhost:27017"
    MONGO_DB_FLAT = "cineexplorer_flat"

    # Batch size
    BATCH_SIZE = 1000

    # Tables
    TABLES = [
        "PERSONS", "MOVIES", "RATINGS", "GENRES", "PRINCIPALS",
        "DIRECTORS", "WRITERS", "CHARACTERS", "TITLES",
        "EPISODES", "PROFESSIONS", "KNOWNFORMOVIES",
    ]

    COLLECTION_NAMES = {
        "PERSONS": "persons", "MOVIES": "movies", "RATINGS": "ratings",
        "GENRES": "genres", "PRINCIPALS": "principals", "DIRECTORS": "directors",
        "WRITERS": "writers", "CHARACTERS": "characters", "TITLES": "titles",
        "EPISODES": "episodes", "PROFESSIONS": "professions",
        "KNOWNFORMOVIES": "knownformovies",
    }


# Couleurs
class Colors:
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    OKBLUE = '\033[94m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'


# ============================================================================
# MIGRATION CLASS
# ============================================================================

class MigrationFlat:
    """Gestionnaire de migration SQLite → MongoDB (collections plates)"""

    def __init__(self, sqlite_path: str, mongo_url: str = Config.MONGO_URL):
        self.sqlite_path = sqlite_path
        self.mongo_url = mongo_url
        self.sqlite_conn = None
        self.mongo_client = None
        self.mongo_db = None
        self.stats = {
            "tables_migrated": 0,
            "total_documents": 0,
            "migration_time": 0,
            "table_stats": {}
        }

        print(f"\n{Colors.BOLD}{Colors.OKBLUE}")
        print("╔" + "═" * 78 + "╗")
        print("║" + "🎬 MIGRATION SQLITE → MONGODB".center(78) + "║")
        print("╚" + "═" * 78 + "╝")
        print(Colors.ENDC)

    def connect(self) -> bool:
        print(f"\n{Colors.BOLD}📌 ÉTAPE 1: Établir les connexions{Colors.ENDC}\n")

        # SQLite
        try:
            if not os.path.exists(self.sqlite_path):
                print(f"{Colors.FAIL}❌ Fichier SQLite non trouvé: {self.sqlite_path}{Colors.ENDC}")
                print(f"\nChemins essayés:")
                for path in Config.POSSIBLE_PATHS:
                    exists = "✅" if os.path.exists(path) else "❌"
                    print(f"  {exists} {path}")
                return False

            self.sqlite_conn = sqlite3.connect(self.sqlite_path)
            self.sqlite_conn.row_factory = sqlite3.Row
            cursor = self.sqlite_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            table_count = cursor.fetchone()[0]

            print(f"{Colors.OKGREEN}✅ SQLite connecté{Colors.ENDC}")
            print(f"   Chemin: {self.sqlite_path}")
            print(f"   Tables: {table_count}")

        except Exception as e:
            print(f"{Colors.FAIL}❌ Erreur SQLite: {e}{Colors.ENDC}")
            return False

        # MongoDB
        try:
            self.mongo_client = MongoClient(self.mongo_url, serverSelectionTimeoutMS=5000)
            self.mongo_client.admin.command('ping')
            self.mongo_db = self.mongo_client[Config.MONGO_DB_FLAT]

            print(f"{Colors.OKGREEN}✅ MongoDB connecté{Colors.ENDC}")
            print(f"   URL: {self.mongo_url}")
            print(f"   Base: {Config.MONGO_DB_FLAT}")

        except Exception as e:
            print(f"{Colors.FAIL}❌ Erreur MongoDB: {e}{Colors.ENDC}")
            return False

        return True

    def migrate_table(self, table_name: str) -> Tuple[bool, int]:
        collection_name = Config.COLLECTION_NAMES[table_name]

        try:
            collection = self.mongo_db[collection_name]
            collection.delete_many({})

            cursor = self.sqlite_conn.cursor()
            cursor.execute(f"SELECT * FROM {table_name}")
            columns = [description[0] for description in cursor.description]

            documents = []
            total_inserted = 0

            for row in cursor:
                doc = dict(zip(columns, row))
                documents.append(doc)

                if len(documents) >= Config.BATCH_SIZE:
                    result = collection.insert_many(documents)
                    total_inserted += len(result.inserted_ids)
                    documents = []

            if documents:
                result = collection.insert_many(documents)
                total_inserted += len(result.inserted_ids)

            collection.create_index("_id")
            return True, total_inserted

        except Exception as e:
            print(f"{Colors.FAIL}❌ Erreur {table_name}: {e}{Colors.ENDC}")
            return False, 0

    def migrate_all(self):
        print(f"\n{Colors.BOLD}📌 ÉTAPE 2: Migration des tables{Colors.ENDC}\n")
        start_time = time.time()

        for table_name in Config.TABLES:
            collection_name = Config.COLLECTION_NAMES[table_name]
            print(f"⏳ {table_name:20} → {collection_name:20}", end=" ")

            success, count = self.migrate_table(table_name)

            if success:
                print(f"{Colors.OKGREEN}✅ {count:,}{Colors.ENDC}")
                self.stats["table_stats"][table_name] = count
                self.stats["total_documents"] += count
                self.stats["tables_migrated"] += 1
            else:
                print(f"{Colors.FAIL}❌{Colors.ENDC}")

        self.stats["migration_time"] = time.time() - start_time

    def verify_migration(self):
        print(f"\n{Colors.BOLD}📌 ÉTAPE 3: Vérification{Colors.ENDC}\n")

        cursor = self.sqlite_conn.cursor()
        all_match = True

        for table_name in Config.TABLES:
            collection_name = Config.COLLECTION_NAMES[table_name]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            sqlite_count = cursor.fetchone()[0]
            mongo_count = self.mongo_db[collection_name].count_documents({})

            match = sqlite_count == mongo_count
            all_match = all_match and match
            status = f"{Colors.OKGREEN}✅{Colors.ENDC}" if match else f"{Colors.FAIL}❌{Colors.ENDC}"
            print(f"{status} {table_name:20} SQLite: {sqlite_count:>10,}  MongoDB: {mongo_count:>10,}")

        return all_match

    def print_stats(self):
        print(f"\n{Colors.BOLD}📌 ÉTAPE 4: Statistiques{Colors.ENDC}\n")

        print(f"{Colors.BOLD}Résumé:{Colors.ENDC}")
        print(f"  Tables migrées: {self.stats['tables_migrated']}/{len(Config.TABLES)}")
        print(f"  Documents: {self.stats['total_documents']:,}")
        print(f"  Temps: {self.stats['migration_time']:.2f} secondes")

        try:
            stats = self.mongo_db.command("dbStats")
            data_size_mb = stats.get("dataSize", 0) / 1024 / 1024
            print(f"  Taille MongoDB: {data_size_mb:.2f} MB")
        except:
            pass

    def close(self):
        if self.sqlite_conn:
            self.sqlite_conn.close()
        if self.mongo_client:
            self.mongo_client.close()


# ============================================================================
# MAIN
# ============================================================================

def main():
    if not PYMONGO_AVAILABLE:
        print(f"{Colors.FAIL}❌ PyMongo n'est pas installé{Colors.ENDC}")
        return 1

    migration = MigrationFlat(Config.DB_SQLITE, Config.MONGO_URL)

    try:
        if not migration.connect():
            return 1

        migration.migrate_all()

        if migration.verify_migration():
            print(f"\n{Colors.OKGREEN}{Colors.BOLD}✅ Vérification réussie!{Colors.ENDC}")
        else:
            print(f"\n{Colors.FAIL}⚠️  Certaines tables ne correspondent pas{Colors.ENDC}")

        migration.print_stats()

        print(f"\n{Colors.BOLD}{Colors.OKGREEN}")
        print("╔" + "═" * 78 + "╗")
        print("║" + "✅ MIGRATION RÉUSSIE!".center(78) + "║")
        print("╚" + "═" * 78 + "╝")
        print(Colors.ENDC)

        return 0

    except Exception as e:
        print(f"{Colors.FAIL}❌ Erreur: {e}{Colors.ENDC}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        migration.close()


if __name__ == '__main__':
    import sys

    sys.exit(main())