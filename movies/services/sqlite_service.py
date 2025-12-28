"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - SQLite Service                                          ║
║                                                                                ║
║     Service d'accès à la base de données SQLite (Phase 1)                     ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from contextlib import contextmanager

from django.conf import settings


class SQLiteService:
    """Service pour les requêtes SQLite"""
    
    def __init__(self):
        self.db_path = settings.DATABASES['default']['NAME']
    
    @contextmanager
    def get_connection(self):
        """Context manager pour les connexions SQLite"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    # =========================================================================
    # STATISTIQUES GÉNÉRALES
    # =========================================================================
    
    def get_stats(self) -> Dict[str, int]:
        """Récupère les statistiques générales de la base"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            stats = {}
            
            # Nombre de films
            cursor.execute("SELECT COUNT(*) FROM MOVIES")
            stats['movies_count'] = cursor.fetchone()[0]
            
            # Nombre de personnes
            cursor.execute("SELECT COUNT(*) FROM PERSONS")
            stats['persons_count'] = cursor.fetchone()[0]
            
            # Nombre de réalisateurs uniques
            cursor.execute("SELECT COUNT(DISTINCT person_id) FROM DIRECTORS")
            stats['directors_count'] = cursor.fetchone()[0]
            
            # Nombre d'acteurs uniques
            cursor.execute("SELECT COUNT(DISTINCT person_id) FROM PRINCIPALS")
            stats['actors_count'] = cursor.fetchone()[0]
            
            # Nombre de genres
            cursor.execute("SELECT COUNT(DISTINCT genre) FROM GENRES")
            stats['genres_count'] = cursor.fetchone()[0]
            
            # Nombre de ratings
            cursor.execute("SELECT COUNT(*) FROM RATINGS")
            stats['ratings_count'] = cursor.fetchone()[0]
            
            return stats
    
    # =========================================================================
    # FILMS
    # =========================================================================
    
    def get_movies(self, limit: int = 20, offset: int = 0, 
                   genre: str = None, year_min: int = None, year_max: int = None,
                   min_rating: float = None, order_by: str = 'primary_title',
                   order_dir: str = 'ASC') -> List[Dict]:
        """
        Récupère une liste de films avec filtres et pagination
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Construction de la requête
            query = """
                SELECT DISTINCT m.movie_id, m.primary_title, m.start_year, 
                       m.runtime_minutes, r.average_rating, r.num_votes
                FROM MOVIES m
                LEFT JOIN RATINGS r ON m.movie_id = r.movie_id
            """
            
            conditions = []
            params = []
            
            # Filtre par genre
            if genre:
                query += " JOIN GENRES g ON m.movie_id = g.movie_id"
                conditions.append("g.genre = ?")
                params.append(genre)
            
            # Filtre par année
            if year_min:
                conditions.append("m.start_year >= ?")
                params.append(year_min)
            if year_max:
                conditions.append("m.start_year <= ?")
                params.append(year_max)
            
            # Filtre par note minimale
            if min_rating:
                conditions.append("r.average_rating >= ?")
                params.append(min_rating)
            
            # Ajout des conditions
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            # Tri
            valid_columns = ['primary_title', 'start_year', 'average_rating', 'num_votes']
            if order_by not in valid_columns:
                order_by = 'primary_title'
            order_dir = 'DESC' if order_dir.upper() == 'DESC' else 'ASC'
            
            query += f" ORDER BY {order_by} {order_dir}"
            
            # Pagination
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])
            
            cursor.execute(query, params)
            
            movies = []
            for row in cursor.fetchall():
                movies.append({
                    'movie_id': row['movie_id'],
                    'title': row['primary_title'],
                    'year': row['start_year'],
                    'runtime': row['runtime_minutes'],
                    'rating': row['average_rating'],
                    'votes': row['num_votes']
                })
            
            return movies
    
    def get_movies_count(self, genre: str = None, year_min: int = None, 
                         year_max: int = None, min_rating: float = None) -> int:
        """Compte le nombre total de films avec les filtres appliqués"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT COUNT(DISTINCT m.movie_id) FROM MOVIES m"
            
            conditions = []
            params = []
            
            if genre:
                query += " JOIN GENRES g ON m.movie_id = g.movie_id"
                conditions.append("g.genre = ?")
                params.append(genre)
            
            if min_rating:
                query += " LEFT JOIN RATINGS r ON m.movie_id = r.movie_id"
                conditions.append("r.average_rating >= ?")
                params.append(min_rating)
            
            if year_min:
                conditions.append("m.start_year >= ?")
                params.append(year_min)
            if year_max:
                conditions.append("m.start_year <= ?")
                params.append(year_max)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            cursor.execute(query, params)
            return cursor.fetchone()[0]
    
    def get_movie_by_id(self, movie_id: str) -> Optional[Dict]:
        """Récupère un film par son ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT m.*, r.average_rating, r.num_votes
                FROM MOVIES m
                LEFT JOIN RATINGS r ON m.movie_id = r.movie_id
                WHERE m.movie_id = ?
            """, (movie_id,))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    
    def get_top_movies(self, limit: int = 10, min_votes: int = 1000) -> List[Dict]:
        """Récupère les meilleurs films par note"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT m.movie_id, m.primary_title, m.start_year, 
                       r.average_rating, r.num_votes
                FROM MOVIES m
                JOIN RATINGS r ON m.movie_id = r.movie_id
                WHERE r.num_votes >= ?
                ORDER BY r.average_rating DESC, r.num_votes DESC
                LIMIT ?
            """, (min_votes, limit))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # GENRES
    # =========================================================================
    
    def get_all_genres(self) -> List[str]:
        """Récupère tous les genres distincts"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT genre FROM GENRES ORDER BY genre")
            return [row[0] for row in cursor.fetchall()]
    
    def get_genres_for_movie(self, movie_id: str) -> List[str]:
        """Récupère les genres d'un film"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT genre FROM GENRES WHERE movie_id = ?
            """, (movie_id,))
            return [row[0] for row in cursor.fetchall()]
    
    def get_movies_by_genre_stats(self) -> List[Dict]:
        """Statistiques des films par genre"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT g.genre, COUNT(*) as count, 
                       AVG(r.average_rating) as avg_rating
                FROM GENRES g
                LEFT JOIN RATINGS r ON g.movie_id = r.movie_id
                GROUP BY g.genre
                ORDER BY count DESC
            """)
            
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # PERSONNES (Acteurs, Réalisateurs)
    # =========================================================================
    
    def get_person_by_id(self, person_id: str) -> Optional[Dict]:
        """Récupère une personne par son ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM PERSONS WHERE person_id = ?
            """, (person_id,))
            
            row = cursor.fetchone()
            if row:
                return dict(row)
            return None
    
    def search_persons(self, name: str, limit: int = 20) -> List[Dict]:
        """Recherche des personnes par nom"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT person_id, name, birth_year, death_year
                FROM PERSONS
                WHERE name LIKE ?
                ORDER BY name
                LIMIT ?
            """, (f'%{name}%', limit))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_actor_filmography(self, actor_name: str) -> List[Dict]:
        """
        Q1: Filmographie d'un acteur
        Retourne les films dans lesquels un acteur a joué
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT m.movie_id, m.primary_title as title, m.start_year as year,
                       c.name as character, r.average_rating as rating
                FROM MOVIES m
                JOIN PRINCIPALS p ON m.movie_id = p.movie_id
                JOIN PERSONS pe ON p.person_id = pe.person_id
                LEFT JOIN CHARACTERS c ON m.movie_id = c.movie_id 
                    AND p.person_id = c.person_id
                LEFT JOIN RATINGS r ON m.movie_id = r.movie_id
                WHERE pe.name LIKE ?
                ORDER BY m.start_year DESC
            """, (f'%{actor_name}%',))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_directors_for_movie(self, movie_id: str) -> List[Dict]:
        """Récupère les réalisateurs d'un film"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT p.person_id, p.name
                FROM DIRECTORS d
                JOIN PERSONS p ON d.person_id = p.person_id
                WHERE d.movie_id = ?
            """, (movie_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_cast_for_movie(self, movie_id: str) -> List[Dict]:
        """Récupère le casting d'un film"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT p.person_id, p.name, c.name as character
                FROM PRINCIPALS pr
                JOIN PERSONS p ON pr.person_id = p.person_id
                LEFT JOIN CHARACTERS c ON pr.movie_id = c.movie_id 
                    AND pr.person_id = c.person_id
                WHERE pr.movie_id = ?
                ORDER BY pr.ordering
            """, (movie_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # RECHERCHE
    # =========================================================================
    
    def search_movies(self, query: str, limit: int = 20) -> List[Dict]:
        """Recherche des films par titre"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT m.movie_id, m.primary_title, m.start_year, 
                       r.average_rating, r.num_votes
                FROM MOVIES m
                LEFT JOIN RATINGS r ON m.movie_id = r.movie_id
                WHERE m.primary_title LIKE ?
                ORDER BY r.num_votes DESC NULLS LAST
                LIMIT ?
            """, (f'%{query}%', limit))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # =========================================================================
    # STATISTIQUES AVANCÉES
    # =========================================================================
    
    def get_movies_by_decade(self) -> List[Dict]:
        """Statistiques des films par décennie"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT (start_year / 10) * 10 as decade, 
                       COUNT(*) as count,
                       AVG(r.average_rating) as avg_rating
                FROM MOVIES m
                LEFT JOIN RATINGS r ON m.movie_id = r.movie_id
                WHERE start_year IS NOT NULL
                GROUP BY decade
                ORDER BY decade
            """)
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_rating_distribution(self) -> List[Dict]:
        """Distribution des notes"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT ROUND(average_rating) as rating_bucket, COUNT(*) as count
                FROM RATINGS
                GROUP BY rating_bucket
                ORDER BY rating_bucket
            """)
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_top_actors(self, limit: int = 10) -> List[Dict]:
        """Top acteurs par nombre de films"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT p.person_id, p.name, COUNT(DISTINCT pr.movie_id) as movie_count
                FROM PERSONS p
                JOIN PRINCIPALS pr ON p.person_id = pr.person_id
                GROUP BY p.person_id, p.name
                ORDER BY movie_count DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]


# Instance singleton pour faciliter l'import
sqlite_service = SQLiteService()
