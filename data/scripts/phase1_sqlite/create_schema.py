#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
CineExplorer - CREATE_SCHEMA.PY (VERSION CORRIGÉE)
================================================================================

Utilité:
  Créer le schéma SQLite normalisé (3NF) SANS INDEXES
  (Les indexes seront créés lors de T1.4 Indexation et benchmark)

Auteur: Équipe CineExplorer
Date: 2025-12-13
Version: 1.1 (CORRIGÉE - Sans indexes)

Usage:
  python scripts/phase1_sqlite/create_schema.py

Résultat:
  Création de data/imdb.db avec 12 tables normalisées (SANS indexes)

NOTE IMPORTANTE:
  ✅ Les 12 tables sont créées
  ❌ Les indexes NE sont PAS créés (voir T1.4 pour la création)

  Cela permet de:
  1. Mesurer les performances SANS index
  2. Ajouter les indexes progressivement
  3. Benchmarker et calculer le gain
"""

import sqlite3
import sys
import os
from pathlib import Path
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# Chemin vers la base de données
DB_PATH = Path(__file__).parent.parent.parent / "data" / "imdb.db"


# Couleurs pour l'affichage
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# ============================================================================
# CLASSE GESTIONNAIRE DE SCHÉMA
# ============================================================================

class SchemaManager:
    """Gère la création du schéma SQLite"""

    def __init__(self, db_path):
        """Initialiser le gestionnaire"""
        self.db_path = db_path
        self.connection = None
        self.cursor = None
        self.stats = {
            'tables_created': 0,
            'indexes_created': 0,
            'errors': 0,
        }

    # ========================================================================
    # CONNEXION
    # ========================================================================

    def connect(self):
        """Établir la connexion à SQLite"""
        try:
            # Créer le répertoire data s'il n'existe pas
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

            self.connection = sqlite3.connect(str(self.db_path))
            self.cursor = self.connection.cursor()

            # Configurer SQLite pour de meilleures performances
            self.cursor.execute("PRAGMA journal_mode = WAL")
            self.cursor.execute("PRAGMA synchronous = NORMAL")
            self.cursor.execute("PRAGMA cache_size = -64000")
            self.cursor.execute("PRAGMA foreign_keys = ON")

            self.print_success(f"✅ Connexion à {self.db_path} établie")
            return True
        except Exception as err:
            self.print_error(f"❌ Erreur de connexion: {err}")
            return False

    def disconnect(self):
        """Fermer la connexion"""
        if self.connection:
            self.connection.close()
            self.print_success("✅ Connexion fermée")

    def commit(self):
        """Valider les changements"""
        self.connection.commit()

    # ========================================================================
    # GESTION DES TABLES
    # ========================================================================

    def execute_sql(self, sql):
        """Exécuter une requête SQL"""
        try:
            self.cursor.execute(sql)
            return True
        except Exception as err:
            self.print_error(f"❌ Erreur SQL: {err}")
            self.stats['errors'] += 1
            return False

    def create_table(self, table_name, create_sql):
        """Créer une table"""
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.cursor.execute(create_sql)
            self.stats['tables_created'] += 1
            self.print_success(f"✅ Table {table_name:20s} créée")
            return True
        except Exception as err:
            self.print_error(f"❌ Table {table_name}: {err}")
            self.stats['errors'] += 1
            return False

    # ========================================================================
    # AFFICHAGE
    # ========================================================================

    @staticmethod
    def print_header(text):
        """Afficher un en-tête"""
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}📊 {text}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

    @staticmethod
    def print_success(text):
        """Afficher un succès"""
        print(f"{Colors.OKGREEN}{text}{Colors.ENDC}")

    @staticmethod
    def print_error(text):
        """Afficher une erreur"""
        print(f"{Colors.FAIL}{text}{Colors.ENDC}")

    @staticmethod
    def print_warning(text):
        """Afficher un avertissement"""
        print(f"{Colors.WARNING}{text}{Colors.ENDC}")

    @staticmethod
    def print_info(text):
        """Afficher une info"""
        print(f"{Colors.OKCYAN}{text}{Colors.ENDC}")

    def print_stats(self):
        """Afficher les statistiques"""
        print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}📈 STATISTIQUES{Colors.ENDC}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
        print(f"✅ Tables créées:   {self.stats['tables_created']}")
        print(f"⏭️  Indexes:        À CRÉER EN T1.4 (Indexation et benchmark)")
        print(f"❌ Erreurs:         {self.stats['errors']}")
        print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}\n")

    # ========================================================================
    # CRÉATION DU SCHÉMA
    # ========================================================================

    def create_schema(self):
        """Créer tout le schéma (SANS INDEXES)"""
        self.print_header("CRÉATION DU SCHÉMA SQLite (SANS INDEXES)")

        self.print_warning("⚠️  ATTENTION: Les indexes NE seront PAS créés ici")
        self.print_warning("    Ils seront ajoutés progressivement en T1.4")
        self.print_warning("    pour benchmarker avec et sans index\n")

        if not self.connect():
            return False

        try:
            # Créer les tables
            self.print_info("1️⃣  Création des 12 tables (SANS INDEXES)...")
            self.create_tables()
            self.commit()

            # Afficher les statistiques
            self.print_stats()

            return self.stats['errors'] == 0

        except Exception as err:
            self.print_error(f"❌ Erreur inattendue: {err}")
            return False
        finally:
            self.disconnect()

    def create_tables(self):
        """Créer toutes les tables SANS INDEXES"""

        # ====================================================================
        # TABLE PERSONS
        # ====================================================================
        self.create_table('PERSONS', """
                                     CREATE TABLE PERSONS
                                     (
                                         person_id  TEXT PRIMARY KEY,
                                         name       TEXT NOT NULL,
                                         birth_year INTEGER,
                                         death_year INTEGER,
                                         CONSTRAINT chk_years CHECK (birth_year <= death_year OR death_year IS NULL)
                                     )
                                     """)

        # ====================================================================
        # TABLE MOVIES
        # ====================================================================
        self.create_table('MOVIES', """
                                    CREATE TABLE MOVIES
                                    (
                                        movie_id        TEXT PRIMARY KEY,
                                        title_type      TEXT    NOT NULL,
                                        primary_title   TEXT    NOT NULL,
                                        original_title  TEXT,
                                        is_adult        INTEGER DEFAULT 0,
                                        start_year      INTEGER NOT NULL,
                                        end_year        INTEGER,
                                        runtime_minutes INTEGER,
                                        CONSTRAINT chk_runtime CHECK (runtime_minutes > 0 OR runtime_minutes IS NULL),
                                        CONSTRAINT chk_years CHECK (start_year <= end_year OR end_year IS NULL)
                                    )
                                    """)

        # ====================================================================
        # TABLE RATINGS
        # ====================================================================
        self.create_table('RATINGS', """
                                     CREATE TABLE RATINGS
                                     (
                                         movie_id       TEXT PRIMARY KEY,
                                         average_rating REAL    NOT NULL,
                                         num_votes      INTEGER NOT NULL,
                                         FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                         CONSTRAINT chk_rating CHECK (average_rating >= 1.0 AND average_rating <= 10.0),
                                         CONSTRAINT chk_votes CHECK (num_votes >= 0)
                                     )
                                     """)

        # ====================================================================
        # TABLE GENRES
        # ====================================================================
        self.create_table('GENRES', """
                                    CREATE TABLE GENRES
                                    (
                                        movie_id TEXT NOT NULL,
                                        genre    TEXT NOT NULL,
                                        PRIMARY KEY (movie_id, genre),
                                        FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE
                                    )
                                    """)

        # ====================================================================
        # TABLE TITLES
        # ====================================================================
        self.create_table('TITLES', """
                                    CREATE TABLE TITLES
                                    (
                                        movie_id          TEXT    NOT NULL,
                                        ordering          INTEGER NOT NULL,
                                        title             TEXT    NOT NULL,
                                        region            TEXT,
                                        language          TEXT,
                                        types             TEXT,
                                        attributes        TEXT,
                                        is_original_title INTEGER,
                                        PRIMARY KEY (movie_id, ordering),
                                        FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE
                                    )
                                    """)

        # ====================================================================
        # TABLE PROFESSIONS
        # ====================================================================
        self.create_table('PROFESSIONS', """
                                         CREATE TABLE PROFESSIONS
                                         (
                                             person_id TEXT NOT NULL,
                                             job_name  TEXT NOT NULL,
                                             PRIMARY KEY (person_id, job_name),
                                             FOREIGN KEY (person_id) REFERENCES PERSONS (person_id) ON DELETE CASCADE
                                         )
                                         """)

        # ====================================================================
        # TABLE PRINCIPALS
        # ====================================================================
        self.create_table('PRINCIPALS', """
                                        CREATE TABLE PRINCIPALS
                                        (
                                            movie_id  TEXT    NOT NULL,
                                            ordering  INTEGER NOT NULL,
                                            person_id TEXT    NOT NULL,
                                            category  TEXT    NOT NULL,
                                            job       TEXT,
                                            PRIMARY KEY (movie_id, ordering, person_id),
                                            FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                            FOREIGN KEY (person_id) REFERENCES PERSONS (person_id) ON DELETE CASCADE
                                        )
                                        """)

        # ====================================================================
        # TABLE DIRECTORS
        # ====================================================================
        self.create_table('DIRECTORS', """
                                       CREATE TABLE DIRECTORS
                                       (
                                           movie_id  TEXT NOT NULL,
                                           person_id TEXT NOT NULL,
                                           PRIMARY KEY (movie_id, person_id),
                                           FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                           FOREIGN KEY (person_id) REFERENCES PERSONS (person_id) ON DELETE CASCADE
                                       )
                                       """)

        # ====================================================================
        # TABLE WRITERS
        # ====================================================================
        self.create_table('WRITERS', """
                                     CREATE TABLE WRITERS
                                     (
                                         movie_id  TEXT NOT NULL,
                                         person_id TEXT NOT NULL,
                                         PRIMARY KEY (movie_id, person_id),
                                         FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                         FOREIGN KEY (person_id) REFERENCES PERSONS (person_id) ON DELETE CASCADE
                                     )
                                     """)

        # ====================================================================
        # TABLE CHARACTERS
        # ====================================================================
        self.create_table('CHARACTERS', """
                                        CREATE TABLE CHARACTERS
                                        (
                                            movie_id  TEXT NOT NULL,
                                            person_id TEXT NOT NULL,
                                            name      TEXT NOT NULL,
                                            PRIMARY KEY (movie_id, person_id, name),
                                            FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                            FOREIGN KEY (person_id) REFERENCES PERSONS (person_id) ON DELETE CASCADE
                                        )
                                        """)

        # ====================================================================
        # TABLE KNOWNFORMOVIES
        # ====================================================================
        self.create_table('KNOWNFORMOVIES', """
                                            CREATE TABLE KNOWNFORMOVIES
                                            (
                                                person_id TEXT NOT NULL,
                                                movie_id  TEXT NOT NULL,
                                                PRIMARY KEY (person_id, movie_id),
                                                FOREIGN KEY (person_id) REFERENCES PERSONS (person_id) ON DELETE CASCADE,
                                                FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE
                                            )
                                            """)

        # ====================================================================
        # TABLE EPISODES
        # ====================================================================
        self.create_table('EPISODES', """
                                      CREATE TABLE EPISODES
                                      (
                                          movie_id        TEXT PRIMARY KEY,
                                          parent_movie_id TEXT    NOT NULL,
                                          season_number   INTEGER NOT NULL,
                                          episode_number  INTEGER NOT NULL,
                                          FOREIGN KEY (movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                          FOREIGN KEY (parent_movie_id) REFERENCES MOVIES (movie_id) ON DELETE CASCADE,
                                          CONSTRAINT chk_season CHECK (season_number > 0),
                                          CONSTRAINT chk_episode CHECK (episode_number > 0)
                                      )
                                      """)


# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    """Fonction principale"""

    print(f"\n{Colors.BOLD}{Colors.OKBLUE}")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "🎬 CINEEXPLORER - CRÉATION DU SCHÉMA SQLITE (SANS INDEXES)".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝")
    print(Colors.ENDC)

    print(f"📅 Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Chemin: {DB_PATH}")
    print(f"🐍 Python: {sys.version.split()[0]}")

    # Créer le gestionnaire
    manager = SchemaManager(DB_PATH)

    try:
        # Créer le schéma
        success = manager.create_schema()

        if success:
            print(f"{Colors.OKGREEN}{Colors.BOLD}")
            print("╔" + "═" * 78 + "╗")
            print("║" + " " * 78 + "║")
            print("║" + "✨ SCHÉMA CRÉÉ SANS INDEXES! ✨".center(78) + "║")
            print("║" + " " * 78 + "║")
            print("╚" + "═" * 78 + "╝")
            print(Colors.ENDC)

            print(f"\n{Colors.WARNING}⚠️  RAPPEL IMPORTANT:{Colors.ENDC}")
            print(f"   • Les 12 tables sont créées")
            print(f"   • Les INDEXES ne sont PAS créés")
            print(f"   • Cela permet de benchmarker EN T1.4")
            print(f"   • Vous ajouterez les indexes progressivement")
            print(f"   • Vous mesurerez le gain (avec/sans index)")

            return 0
        else:
            print(f"\n{Colors.FAIL}{Colors.BOLD}❌ ERREUR LORS DE LA CRÉATION{Colors.ENDC}")
            return 1

    except KeyboardInterrupt:
        print(f"\n{Colors.WARNING}⚠️  Opération annulée par l'utilisateur{Colors.ENDC}")
        return 1
    except Exception as err:
        print(f"{Colors.FAIL}❌ Erreur inattendue: {err}{Colors.ENDC}")
        return 1


# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == '__main__':
    sys.exit(main())