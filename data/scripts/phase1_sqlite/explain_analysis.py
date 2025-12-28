#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
CineExplorer - EXPLAIN_ANALYSIS.PY (Analyse des plans d'exécution)
================================================================================

Utilité:
  Analyser les plans d'exécution (EXPLAIN QUERY PLAN) pour chaque requête
  AVANT et APRÈS création des indexes

Auteur: Équipe CineExplorer
Date: 2025-12-14
Version: 1.0

Usage:
  python scripts/explain_analysis.py
"""

import sqlite3
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

DB_PATH = Path(__file__).parent.parent.parent / "data" / "imdb.db"


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
# REQUÊTES À ANALYSER
# ============================================================================

QUERIES_TO_ANALYZE = {
    "1️⃣  Actor Filmography": """
                              SELECT DISTINCT m.primary_title                   AS titre,
                                              m.start_year                      AS année,
                                              GROUP_CONCAT(g.genre, ', ')       AS genres,
                                              COALESCE(r.average_rating, 'N/A') AS note
                              FROM movies m
                                       JOIN principals p ON m.movie_id = p.movie_id
                                       JOIN persons pe ON p.person_id = pe.person_id
                                       LEFT JOIN genres g ON m.movie_id = g.movie_id
                                       LEFT JOIN ratings r ON m.movie_id = r.movie_id
                              WHERE pe.name LIKE '%Tom Hanks%'
                              GROUP BY m.movie_id
                              ORDER BY m.start_year DESC
                              """,

    "2️⃣  Top N Films": """
                        SELECT ROW_NUMBER()        OVER (ORDER BY r.average_rating DESC) AS rang, m.primary_title AS titre,
                               m.start_year     AS année,
                               r.average_rating AS note,
                               r.num_votes      AS votes
                        FROM movies m
                                 JOIN genres g ON m.movie_id = g.movie_id
                                 JOIN ratings r ON m.movie_id = r.movie_id
                        WHERE g.genre = 'Drama'
                          AND m.start_year >= 2010
                          AND m.start_year <= 2024
                          AND r.num_votes > 0
                        ORDER BY r.average_rating DESC LIMIT 10
                        """,

    "3️⃣  Multi-Role Actors": """
                              SELECT pe.name                AS acteur,
                                     m.primary_title        AS film,
                                     COUNT(DISTINCT c.name) AS nombre_rôles
                              FROM characters c
                                       JOIN persons pe ON c.person_id = pe.person_id
                                       JOIN movies m ON c.movie_id = m.movie_id
                              GROUP BY c.movie_id, c.person_id
                              HAVING COUNT(DISTINCT c.name) > 1
                              ORDER BY nombre_rôles DESC
                              """,

    "4️⃣  Collaborations": """
                           SELECT pe.name                    AS réalisateur,
                                  COUNT(DISTINCT d.movie_id) AS nombre_films
                           FROM directors d
                                    JOIN persons pe ON d.person_id = pe.person_id
                           WHERE d.movie_id IN (SELECT DISTINCT p.movie_id
                                                FROM principals p
                                                         JOIN persons actor ON p.person_id = actor.person_id
                                                WHERE actor.name LIKE '%Steven Spielberg%')
                           GROUP BY d.person_id
                           ORDER BY nombre_films DESC
                           """,

    "5️⃣  Popular Genres": """
                           SELECT g.genre,
                                  ROUND(AVG(r.average_rating), 2) AS note_moyenne,
                                  COUNT(DISTINCT g.movie_id)      AS nombre_films
                           FROM genres g
                                    JOIN ratings r ON g.movie_id = r.movie_id
                           GROUP BY g.genre
                           HAVING AVG(r.average_rating) > 7.0
                              AND COUNT(DISTINCT g.movie_id) > 50
                           ORDER BY note_moyenne DESC
                           """,
}


# ============================================================================
# ANALYSE
# ============================================================================

class ExplainAnalyzer:
    """Analyse les plans d'exécution"""

    def __init__(self, db_path: Path):
        """Initialiser"""
        self.db_path = db_path
        self.conn = None

    def connect(self) -> bool:
        """Établir la connexion"""
        try:
            self.conn = sqlite3.connect(str(self.db_path))
            return True
        except Exception as e:
            print(f"{Colors.FAIL}❌ Erreur: {e}{Colors.ENDC}")
            return False

    def disconnect(self) -> None:
        """Fermer la connexion"""
        if self.conn:
            self.conn.close()

    def get_query_plan(self, sql: str) -> str:
        """Obtenir le plan d'exécution"""
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"EXPLAIN QUERY PLAN {sql}")
            rows = cursor.fetchall()
            return "\n".join(str(row) for row in rows)
        except Exception as e:
            return f"Erreur: {e}"

    def analyze(self) -> None:
        """Analyser tous les plans"""

        if not self.connect():
            return

        try:
            print(f"\n{Colors.BOLD}{Colors.OKBLUE}")
            print("╔" + "═" * 78 + "╗")
            print("║" + " " * 78 + "║")
            print("║" + "🔍 ANALYSE DES PLANS D'EXÉCUTION".center(78) + "║")
            print("║" + " " * 78 + "║")
            print("╚" + "═" * 78 + "╝")
            print(Colors.ENDC)

            for query_label, sql in QUERIES_TO_ANALYZE.items():
                print(f"\n{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")
                print(f"{Colors.BOLD}{Colors.OKBLUE}{query_label}{Colors.ENDC}")
                print(f"{Colors.BOLD}{Colors.OKBLUE}{'=' * 80}{Colors.ENDC}")

                plan = self.get_query_plan(sql)
                print(f"\n{Colors.OKCYAN}{plan}{Colors.ENDC}\n")

                # Analyser le plan
                self.analyze_plan(plan, query_label)

        finally:
            self.disconnect()

    def analyze_plan(self, plan: str, query_label: str) -> None:
        """Analyser et commenter le plan"""

        print(f"{Colors.WARNING}💡 Analyse:{Colors.ENDC}")

        if "SCAN TABLE" in plan and "INDEX" not in plan:
            print(f"   ⚠️  FULL TABLE SCAN détecté")
            print(f"      → Considérer un index sur les colonnes de jointure")

        if "FULL SCAN" in plan:
            print(f"   ⚠️  FULL SCAN détecté")
            print(f"      → Performance critique, index très recommandé")

        if "SEARCH" in plan and "INDEX" in plan:
            print(f"   ✅ Index utilisé (SEARCH)")
            print(f"      → Bonne performance attendue")

        if "HASH JOIN" in plan or "NESTED LOOP" in plan:
            print(f"   📊 Type de join: {plan.split()[0] if plan else 'Inconnu'}")


# ============================================================================
# FONCTION PRINCIPALE
# ============================================================================

def main():
    """Fonction principale"""

    if not DB_PATH.exists():
        print(f"{Colors.FAIL}❌ Base de données introuvable: {DB_PATH}{Colors.ENDC}")
        return 1

    analyzer = ExplainAnalyzer(DB_PATH)
    analyzer.analyze()

    return 0


# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == '__main__':
    import sys

    sys.exit(main())