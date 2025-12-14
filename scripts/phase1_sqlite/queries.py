#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
CineExplorer - QUERIES.PY
================================================================================

Utilité:
  Implémente les 9 requêtes SQL analytiques sur la base IMDB

Auteur: Équipe CineExplorer
Date: 2025-12-13
Version: 1.0

Format:
  Chaque requête est une fonction documentée retournant les résultats

Usage:
  from queries import query_actor_filmography
  results = query_actor_filmography(conn, "Tom Hanks")
"""

import sqlite3
from typing import List, Tuple, Optional


# ============================================================================
# REQUÊTE 1: Filmographie d'un acteur
# ============================================================================

def query_actor_filmography(conn: sqlite3.Connection, actor_name: str) -> List[Tuple]:
    """
    Retourne la filmographie d'un acteur donné.

    Args:
        conn: Connexion SQLite
        actor_name: Nom de l'acteur (ex: "Tom Hanks")

    Returns:
        Liste de tuples (titre, année, genre, note)

    SQL utilisé:
        SELECT ... FROM ... WHERE ... ORDER BY ...
    """
    sql = """
          SELECT DISTINCT m.primary_title                   AS titre, \
                          m.start_year                      AS année, \
                          GROUP_CONCAT(g.genre, ', ')       AS genres, \
                          COALESCE(r.average_rating, 'N/A') AS note
          FROM movies m
                   JOIN principals p ON m.movie_id = p.movie_id
                   JOIN persons pe ON p.person_id = pe.person_id
                   LEFT JOIN genres g ON m.movie_id = g.movie_id
                   LEFT JOIN ratings r ON m.movie_id = r.movie_id
          WHERE pe.name LIKE ?
          GROUP BY m.movie_id
          ORDER BY m.start_year DESC \
          """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()


# ============================================================================
# REQUÊTE 2: Top N films
# ============================================================================

def query_top_n_films(
        conn: sqlite3.Connection,
        genre: str,
        year_start: int,
        year_end: int,
        n: int = 10
) -> List[Tuple]:
    """
    Retourne les N meilleurs films d'un genre sur une période donnée.

    Args:
        conn: Connexion SQLite
        genre: Genre à filtrer (ex: "Drama")
        year_start: Année de début (incluse)
        year_end: Année de fin (incluse)
        n: Nombre de films à retourner (défaut: 10)

    Returns:
        Liste de tuples (rang, titre, année, note, votes)

    SQL utilisé:
        SELECT ... FROM ... JOIN ... WHERE ... ORDER BY ... LIMIT ...
    """
    sql = """
          SELECT ROW_NUMBER()        OVER (ORDER BY r.average_rating DESC) AS rang, m.primary_title AS titre, \
                 m.start_year     AS année, \
                 r.average_rating AS note, \
                 r.num_votes      AS votes
          FROM movies m
                   JOIN genres g ON m.movie_id = g.movie_id
                   JOIN ratings r ON m.movie_id = r.movie_id
          WHERE g.genre = ?
            AND m.start_year >= ?
            AND m.start_year <= ?
            AND r.num_votes > 0
          ORDER BY r.average_rating DESC LIMIT ? \
          """
    return conn.execute(sql, (genre, year_start, year_end, n)).fetchall()


# ============================================================================
# REQUÊTE 3: Acteurs multi-rôles
# ============================================================================

def query_multi_role_actors(conn: sqlite3.Connection) -> List[Tuple]:
    """
    Retourne les acteurs ayant joué plusieurs personnages dans un même film.

    Args:
        conn: Connexion SQLite

    Returns:
        Liste de tuples (nom_acteur, titre_film, nombre_rôles)

    SQL utilisé:
        SELECT ... FROM ... GROUP BY ... HAVING COUNT(*) > 1 ORDER BY ...
    """
    sql = """
          SELECT pe.name                AS acteur, \
                 m.primary_title        AS film, \
                 COUNT(DISTINCT c.name) AS nombre_rôles
          FROM characters c
                   JOIN persons pe ON c.person_id = pe.person_id
                   JOIN movies m ON c.movie_id = m.movie_id
          GROUP BY c.movie_id, c.person_id
          HAVING COUNT(DISTINCT c.name) > 1
          ORDER BY nombre_rôles DESC \
          """
    return conn.execute(sql).fetchall()


# ============================================================================
# REQUÊTE 4: Collaborations (avec sous-requête)
# ============================================================================

def query_collaborations(conn: sqlite3.Connection, actor_name: str) -> List[Tuple]:
    """
    Retourne les réalisateurs ayant travaillé avec un acteur spécifique.

    Args:
        conn: Connexion SQLite
        actor_name: Nom de l'acteur (ex: "Tom Hanks")

    Returns:
        Liste de tuples (réalisateur, nombre_films)

    SQL utilisé:
        SELECT ... FROM ... WHERE movie_id IN (SELECT ...) ORDER BY ...
        (Sous-requête pour trouver les films de l'acteur)
    """
    sql = """
          SELECT pe.name                    AS réalisateur, \
                 COUNT(DISTINCT d.movie_id) AS nombre_films
          FROM directors d
                   JOIN persons pe ON d.person_id = pe.person_id
          WHERE d.movie_id IN (SELECT DISTINCT p.movie_id \
                               FROM principals p \
                                        JOIN persons actor ON p.person_id = actor.person_id \
                               WHERE actor.name LIKE ?)
          GROUP BY d.person_id
          ORDER BY nombre_films DESC \
          """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()


# ============================================================================
# REQUÊTE 5: Genres populaires (GROUP BY + HAVING)
# ============================================================================

def query_popular_genres(conn: sqlite3.Connection, min_rating: float = 7.0, min_count: int = 50) -> List[Tuple]:
    """
    Retourne les genres avec une note moyenne > 7.0 et plus de 50 films.

    Args:
        conn: Connexion SQLite
        min_rating: Note minimale (défaut: 7.0)
        min_count: Nombre minimum de films (défaut: 50)

    Returns:
        Liste de tuples (genre, note_moyenne, nombre_films)

    SQL utilisé:
        SELECT ... FROM ... GROUP BY ... HAVING ... ORDER BY ...
    """
    sql = """
          SELECT g.genre, \
                 ROUND(AVG(r.average_rating), 2) AS note_moyenne, \
                 COUNT(DISTINCT g.movie_id)      AS nombre_films
          FROM genres g
                   JOIN ratings r ON g.movie_id = r.movie_id
          GROUP BY g.genre
          HAVING AVG(r.average_rating) > ? \
             AND COUNT(DISTINCT g.movie_id) > ?
          ORDER BY note_moyenne DESC \
          """
    return conn.execute(sql, (min_rating, min_count)).fetchall()


# ============================================================================
# REQUÊTE 6: Évolution de carrière (CTE - WITH)
# ============================================================================

def query_career_evolution(conn: sqlite3.Connection, actor_name: str) -> List[Tuple]:
    """
    Retourne l'évolution de carrière d'un acteur (films par décennie).

    Args:
        conn: Connexion SQLite
        actor_name: Nom de l'acteur (ex: "Tom Hanks")

    Returns:
        Liste de tuples (décennie, nombre_films, note_moyenne)

    SQL utilisé:
        WITH ... AS (SELECT ...) SELECT ... FROM ... ORDER BY ...
        (CTE pour grouper par décennie)
    """
    sql = """
          WITH career_data AS (SELECT (m.start_year / 10) * 10 AS décennie, \
                                      m.movie_id, \
                                      r.average_rating \
                               FROM movies m \
                                        JOIN principals p ON m.movie_id = p.movie_id \
                                        JOIN persons pe ON p.person_id = pe.person_id \
                                        LEFT JOIN ratings r ON m.movie_id = r.movie_id \
                               WHERE pe.name LIKE ?)
          SELECT décennie || 's'               AS décennie, \
                 COUNT(DISTINCT movie_id)      AS nombre_films, \
                 ROUND(AVG(average_rating), 2) AS note_moyenne
          FROM career_data
          WHERE décennie > 0
          GROUP BY décennie
          ORDER BY décennie \
          """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()


# ============================================================================
# REQUÊTE 7: Classement par genre (RANK/ROW_NUMBER)
# ============================================================================

def query_top_films_per_genre(conn: sqlite3.Connection, top_n: int = 3) -> List[Tuple]:
    """
    Retourne les N meilleurs films pour chaque genre avec leur rang.

    Args:
        conn: Connexion SQLite
        top_n: Nombre de films par genre (défaut: 3)

    Returns:
        Liste de tuples (genre, rang, titre, année, note)

    SQL utilisé:
        SELECT ... FROM (SELECT ... RANK() OVER (PARTITION BY ...)) ...
    """
    sql = """
          SELECT g.genre, \
                 RANK()              OVER (PARTITION BY g.genre ORDER BY r.average_rating DESC) AS rang, m.primary_title AS titre, \
                 m.start_year     AS année, \
                 r.average_rating AS note
          FROM (SELECT g.genre,
                       g.movie_id,
                       r.average_rating,
                       RANK() OVER (PARTITION BY g.genre ORDER BY r.average_rating DESC) AS rk \
                FROM genres g \
                         JOIN ratings r ON g.movie_id = r.movie_id) ranked
                   JOIN genres g ON ranked.movie_id = g.movie_id
                   JOIN movies m ON g.movie_id = m.movie_id
                   JOIN ratings r ON m.movie_id = r.movie_id
          WHERE ranked.rk <= ?
          ORDER BY g.genre, rang \
          """
    return conn.execute(sql, (top_n,)).fetchall()


# ============================================================================
# REQUÊTE 8: Carrière propulsée
# ============================================================================

def query_breakthrough_careers(conn: sqlite3.Connection,
                               threshold_votes: int = 200000) -> List[Tuple]:
    """
    Retourne les acteurs ayant percé grâce à un film.

    Logique correcte:
    - AVANT: acteur avait des films AVEC < 200k votes
    - APRÈS: acteur a eu UN film avec > 200k votes (la "percée")
    - Résultat: Avant et après cette percée

    Version EFFICACE avec CTE:
    - Calcule les films AVANT/APRÈS pour chaque acteur
    - Filtre les acteurs avec progression (avant peu, après gros film)
    - Temps d'exécution: ~2-3 secondes

    Args:
        conn: Connexion SQLite
        threshold_votes: Seuil de votes pour un gros succès (défaut: 200000)

    Returns:
        Liste de tuples (acteur, films_avant, films_après, film_percée, votes, note)

    SQL utilisé:
        WITH ... AS (SELECT ... GROUP BY ...)
        SELECT ... FROM ... WHERE ... ORDER BY ...
    """
    sql = """
          WITH actor_filmography AS (
              -- Pour chaque acteur: films avant et après le seuil
              SELECT pe.person_id, \
                     pe.name                                                        AS acteur, \
                     COUNT(DISTINCT CASE WHEN r.num_votes <= ? THEN p.movie_id END) AS films_avant, \
                     COUNT(DISTINCT CASE WHEN r.num_votes > ? THEN p.movie_id END)  AS films_après, \
                     MAX(CASE WHEN r.num_votes > ? THEN r.num_votes END)            AS max_votes_après, \
                     MAX(CASE WHEN r.num_votes > ? THEN r.average_rating END)       AS max_note_après
              FROM principals p
                       JOIN persons pe ON p.person_id = pe.person_id
                       JOIN ratings r ON p.movie_id = r.movie_id
              GROUP BY pe.person_id)
          SELECT af.acteur, \
                 af.films_avant, \
                 af.films_après, \
                 m.primary_title  AS film_percée, \
                 r.num_votes      AS votes_film, \
                 r.average_rating AS note_film
          FROM actor_filmography af
                   JOIN principals p ON af.person_id = p.person_id
                   JOIN movies m ON p.movie_id = m.movie_id
                   JOIN ratings r ON m.movie_id = r.movie_id
          WHERE af.films_avant > 0
            AND af.films_après > 0
            AND r.num_votes = af.max_votes_après
          ORDER BY r.num_votes DESC LIMIT 50 \
          """
    return conn.execute(sql, (threshold_votes, threshold_votes, threshold_votes, threshold_votes)).fetchall()


# ============================================================================
# REQUÊTE 9: Requête libre - Réalisateurs les plus prolifiques
# ============================================================================

def query_most_prolific_directors(conn: sqlite3.Connection, min_films: int = 10) -> List[Tuple]:
    """
    Retourne les réalisateurs les plus prolifiques avec leur filmographie complète.

    Cette requête combine:
    - 3 JOINs (directors, persons, movies, ratings)
    - GROUP BY avec HAVING
    - Calcul de statistiques

    Question: Quels sont les réalisateurs les plus prolifiques et quelle est
    la qualité globale de leurs films?

    Args:
        conn: Connexion SQLite
        min_films: Nombre minimum de films (défaut: 10)

    Returns:
        Liste de tuples (réalisateur, nombre_films, note_moyenne, note_min, note_max)

    SQL utilisé:
        SELECT ... FROM ... JOIN ... GROUP BY ... HAVING ... ORDER BY ...
    """
    sql = """
          SELECT pe.name                         AS réalisateur, \
                 COUNT(DISTINCT d.movie_id)      AS nombre_films, \
                 ROUND(AVG(r.average_rating), 2) AS note_moyenne, \
                 ROUND(MIN(r.average_rating), 2) AS note_min, \
                 ROUND(MAX(r.average_rating), 2) AS note_max, \
                 COUNT(DISTINCT g.genre)         AS genres_explorés
          FROM directors d
                   JOIN persons pe ON d.person_id = pe.person_id
                   JOIN movies m ON d.movie_id = m.movie_id
                   JOIN ratings r ON m.movie_id = r.movie_id
                   LEFT JOIN genres g ON m.movie_id = g.movie_id
          GROUP BY d.person_id
          HAVING COUNT(DISTINCT d.movie_id) >= ?
          ORDER BY nombre_films DESC LIMIT 30 \
          """
    return conn.execute(sql, (min_films,)).fetchall()


# ============================================================================
# FONCTION UTILITAIRE: Afficher les résultats
# ============================================================================

def print_results(query_name: str, results: List[Tuple], headers: List[str] = None) -> None:
    """
    Affiche les résultats de manière formatée.

    Args:
        query_name: Nom de la requête
        results: Résultats à afficher
        headers: En-têtes optionnels
    """
    print(f"\n{'=' * 80}")
    print(f"📊 {query_name}")
    print(f"{'=' * 80}")

    if not results:
        print("❌ Aucun résultat")
        return

    if headers:
        print(" | ".join(headers))
        print("-" * 80)

    for row in results[:10]:  # Limiter à 10 résultats pour l'affichage
        print(" | ".join(str(x) for x in row))

    if len(results) > 10:
        print(f"... et {len(results) - 10} autres résultats")


# ============================================================================
# EXEMPLE D'UTILISATION
# ============================================================================

if __name__ == '__main__':
    import sys
    from pathlib import Path

    # Chemin vers la base de données (remonte à cineexplorer, puis data/)
    DB_PATH = Path(__file__).parent.parent.parent / "data" / "imdb.db"

    if not DB_PATH.exists():
        print(f"❌ Base de données introuvable: {DB_PATH}")
        sys.exit(1)

    # Connexion
    conn = sqlite3.connect(str(DB_PATH))

    print("\n🎬 CINEEXPLORER - REQUÊTES ANALYTIQUES\n")

    try:
        # 1. Filmographie
        results = query_actor_filmography(conn, "Tom Hanks")
        print_results("1️⃣  Filmographie (Tom Hanks)", results,
                      ["Titre", "Année", "Genres", "Note"])

        # 2. Top films
        results = query_top_n_films(conn, "Drama", 2020, 2024, 5)
        print_results("2️⃣  Top 5 films Drama (2020-2024)", results,
                      ["Rang", "Titre", "Année", "Note", "Votes"])

        # 3. Acteurs multi-rôles
        results = query_multi_role_actors(conn)
        print_results("3️⃣  Acteurs multi-rôles (Top 10)", results[:10],
                      ["Acteur", "Film", "Nombre de rôles"])

        # 4. Collaborations
        results = query_collaborations(conn, "Tom Hanks")
        print_results("4️⃣  Réalisateurs avec Tom Hanks", results,
                      ["Réalisateur", "Nombre de films"])

        # 5. Genres populaires
        results = query_popular_genres(conn)
        print_results("5️⃣  Genres populaires (>7.0, >50 films)", results,
                      ["Genre", "Note moyenne", "Nombre de films"])

        # 6. Évolution carrière
        results = query_career_evolution(conn, "Tom Hanks")
        print_results("6️⃣  Évolution de carrière (Tom Hanks)", results,
                      ["Décennie", "Nombre de films", "Note moyenne"])

        # 7. Classement par genre
        results = query_top_films_per_genre(conn, 3)
        print_results("7️⃣  Top 3 films par genre", results[:15],
                      ["Genre", "Rang", "Titre", "Année", "Note"])

        # 8. Carrière propulsée
        results = query_breakthrough_careers(conn, threshold_votes=200000)
        print_results("8️⃣  Carrières propulsées (avant <200k, après >200k)", results[:10],
                      ["Acteur", "Films avant", "Films après", "Film percée", "Votes", "Note"])

        # 9. Réalisateurs prolifiques
        results = query_most_prolific_directors(conn, 10)
        print_results("9️⃣  Réalisateurs les plus prolifiques (>10 films)", results,
                      ["Réalisateur", "Films", "Note moy", "Min", "Max", "Genres"])

        print(f"\n{'=' * 80}")
        print("✅ Toutes les requêtes ont été exécutées avec succès!")
        print(f"{'=' * 80}\n")

    finally:
        conn.close()