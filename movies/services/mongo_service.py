"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - MongoDB Service                                         ║
║                                                                                ║
║     Service d'accès à MongoDB Replica Set (Phase 2 & 3)                       ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from typing import Dict, List, Optional, Any
from django.conf import settings

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False


class MongoDBService:
    """Service pour les requêtes MongoDB (Replica Set)"""
    
    _instance = None
    _client = None
    
    def __new__(cls):
        """Singleton pattern pour réutiliser la connexion"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not PYMONGO_AVAILABLE:
            raise ImportError("PyMongo n'est pas installé. Lancez: pip install pymongo")
        
        self.config = settings.MONGODB_CONFIG
        self._ensure_connection()
    
    def _ensure_connection(self):
        """Assure qu'une connexion est établie"""
        if self._client is None:
            try:
                self._client = MongoClient(
                    self.config['URI'],
                    **self.config.get('OPTIONS', {})
                )
                # Test de connexion
                self._client.admin.command('ping')
            except (ConnectionFailure, ServerSelectionTimeoutError) as e:
                self._client = None
                raise ConnectionError(f"Impossible de se connecter à MongoDB: {e}")
    
    @property
    def client(self) -> MongoClient:
        """Retourne le client MongoDB"""
        self._ensure_connection()
        return self._client
    
    @property
    def db(self):
        """Retourne la base de données principale"""
        return self.client[self.config['DATABASE_FLAT']]
    
    def get_collection(self, name: str):
        """Retourne une collection par son nom"""
        collection_name = self.config['COLLECTIONS'].get(name, name)
        return self.db[collection_name]
    
    # =========================================================================
    # STATUS & HEALTH
    # =========================================================================
    
    def get_replica_status(self) -> Dict:
        """Récupère le statut du Replica Set"""
        try:
            status = self.client.admin.command('replSetGetStatus')
            return {
                'set': status.get('set'),
                'members': [
                    {
                        'name': m.get('name'),
                        'state': m.get('stateStr'),
                        'health': m.get('health')
                    }
                    for m in status.get('members', [])
                ]
            }
        except Exception as e:
            return {'error': str(e)}
    
    def is_connected(self) -> bool:
        """Vérifie si la connexion est active"""
        try:
            self.client.admin.command('ping')
            return True
        except:
            return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Récupère les statistiques de la base MongoDB"""
        stats = {}
        
        try:
            # Stats de la base
            db_stats = self.db.command('dbStats')
            stats['data_size_mb'] = round(db_stats.get('dataSize', 0) / 1024 / 1024, 2)
            stats['storage_size_mb'] = round(db_stats.get('storageSize', 0) / 1024 / 1024, 2)
            stats['collections_count'] = db_stats.get('collections', 0)
            
            # Comptages des collections principales
            stats['movies_count'] = self.db.movies.count_documents({})
            stats['movies_complete_count'] = self.db.movies_complete.count_documents({})
            stats['persons_count'] = self.db.persons.count_documents({})
            
        except Exception as e:
            stats['error'] = str(e)
        
        return stats
    
    # =========================================================================
    # FILMS (Collection movies_complete - documents structurés)
    # =========================================================================
    
    def get_movie_complete(self, movie_id: str) -> Optional[Dict]:
        """
        Récupère un film complet depuis la collection movies_complete
        (Document dénormalisé avec genres, cast, directors, etc.)
        """
        return self.db.movies_complete.find_one({'_id': movie_id})
    
    def get_movies_complete(self, limit: int = 20, skip: int = 0,
                           genre: str = None, year_min: int = None, 
                           year_max: int = None, min_rating: float = None,
                           sort_by: str = 'title', sort_order: int = 1) -> List[Dict]:
        """
        Récupère une liste de films complets avec filtres
        """
        # Construction du filtre
        query = {}
        
        if genre:
            query['genres'] = genre
        
        if year_min or year_max:
            query['year'] = {}
            if year_min:
                query['year']['$gte'] = year_min
            if year_max:
                query['year']['$lte'] = year_max
            if not query['year']:
                del query['year']
        
        if min_rating:
            query['rating.average'] = {'$gte': min_rating}
        
        # Mapping des champs de tri
        sort_mapping = {
            'title': 'title',
            'year': 'year',
            'rating': 'rating.average',
            'votes': 'rating.votes'
        }
        sort_field = sort_mapping.get(sort_by, 'title')
        
        # Exécution de la requête
        cursor = self.db.movies_complete.find(query) \
            .sort(sort_field, sort_order) \
            .skip(skip) \
            .limit(limit)
        
        return list(cursor)
    
    def count_movies_complete(self, genre: str = None, year_min: int = None,
                              year_max: int = None, min_rating: float = None) -> int:
        """Compte les films avec les filtres appliqués"""
        query = {}
        
        if genre:
            query['genres'] = genre
        
        if year_min or year_max:
            query['year'] = {}
            if year_min:
                query['year']['$gte'] = year_min
            if year_max:
                query['year']['$lte'] = year_max
        
        if min_rating:
            query['rating.average'] = {'$gte': min_rating}
        
        return self.db.movies_complete.count_documents(query)
    
    def get_top_movies(self, limit: int = 10, min_votes: int = 10000) -> List[Dict]:
        """Récupère les meilleurs films par note"""
        return list(self.db.movies_complete.find(
            {'rating.votes': {'$gte': min_votes}}
        ).sort('rating.average', -1).limit(limit))
    
    def search_movies(self, query: str, limit: int = 20) -> List[Dict]:
        """Recherche des films par titre"""
        # Recherche par regex (case-insensitive)
        return list(self.db.movies_complete.find(
            {'title': {'$regex': query, '$options': 'i'}}
        ).sort('rating.votes', -1).limit(limit))
    
    # =========================================================================
    # GENRES
    # =========================================================================
    
    def get_all_genres(self) -> List[str]:
        """Récupère tous les genres distincts"""
        return self.db.movies_complete.distinct('genres')
    
    def get_genres_stats(self) -> List[Dict]:
        """Statistiques par genre avec pipeline d'agrégation"""
        pipeline = [
            {'$unwind': '$genres'},
            {'$group': {
                '_id': '$genres',
                'count': {'$sum': 1},
                'avg_rating': {'$avg': '$rating.average'}
            }},
            {'$sort': {'count': -1}}
        ]
        
        return list(self.db.movies_complete.aggregate(pipeline))
    
    # =========================================================================
    # PERSONNES
    # =========================================================================
    
    def get_person(self, person_id: str) -> Optional[Dict]:
        """Récupère une personne par son ID"""
        return self.db.persons.find_one({'person_id': person_id})
    
    def search_persons(self, name: str, limit: int = 20) -> List[Dict]:
        """Recherche des personnes par nom"""
        return list(self.db.persons.find(
            {'name': {'$regex': name, '$options': 'i'}}
        ).limit(limit))
    
    def get_actor_filmography(self, actor_name: str) -> List[Dict]:
        """Récupère la filmographie d'un acteur"""
        # Recherche dans movies_complete où l'acteur est dans le cast
        return list(self.db.movies_complete.find(
            {'cast.name': {'$regex': actor_name, '$options': 'i'}},
            {'title': 1, 'year': 1, 'rating': 1, 'cast.$': 1}
        ).sort('year', -1))
    
    # =========================================================================
    # STATISTIQUES AVANCÉES (Agrégation Pipeline)
    # =========================================================================
    
    def get_movies_by_decade(self) -> List[Dict]:
        """Statistiques par décennie"""
        pipeline = [
            {'$match': {'year': {'$ne': None}}},
            {'$group': {
                '_id': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]},
                'count': {'$sum': 1},
                'avg_rating': {'$avg': '$rating.average'}
            }},
            {'$sort': {'_id': 1}}
        ]
        
        result = list(self.db.movies_complete.aggregate(pipeline))
        return [{'decade': r['_id'], 'count': r['count'], 'avg_rating': r['avg_rating']} for r in result]
    
    def get_rating_distribution(self) -> List[Dict]:
        """Distribution des notes"""
        pipeline = [
            {'$match': {'rating.average': {'$ne': None}}},
            {'$group': {
                '_id': {'$round': ['$rating.average', 0]},
                'count': {'$sum': 1}
            }},
            {'$sort': {'_id': 1}}
        ]
        
        result = list(self.db.movies_complete.aggregate(pipeline))
        return [{'rating': r['_id'], 'count': r['count']} for r in result]

    def get_top_directors(self, limit: int = 10) -> List[Dict]:
        """Top réalisateurs par nombre de films"""
        pipeline = [
            {'$unwind': '$directors'},
            {'$group': {
                '_id': '$directors.name',
                'movie_count': {'$sum': 1},
                'avg_rating': {'$avg': '$rating.average'}
            }},
            {'$sort': {'movie_count': -1}},
            {'$limit': limit},
            {'$project': {
                'name': '$_id',
                'movie_count': 1,
                'avg_rating': 1,
                '_id': 0
            }}
        ]

        return list(self.db.movies_complete.aggregate(pipeline))
    
    def get_top_actors(self, limit: int = 10) -> List[Dict]:
        """Top acteurs par nombre de films"""
        pipeline = [
            {'$unwind': '$cast'},
            {'$group': {
                '_id': '$cast.name',
                'movie_count': {'$sum': 1},
                'avg_rating': {'$avg': '$rating.average'}
            }},
            {'$sort': {'movie_count': -1}},
            {'$limit': limit}
        ]
        
        return list(self.db.movies_complete.aggregate(pipeline))
    
    # =========================================================================
    # REQUÊTES SPÉCIFIQUES (Phase 2 - Équivalentes SQL)
    # =========================================================================
    
    def get_top_n_movies_by_genre(self, genre: str, year_start: int, 
                                   year_end: int, n: int = 10) -> List[Dict]:
        """
        Q2: Top N films d'un genre sur une période
        """
        return list(self.db.movies_complete.find({
            'genres': genre,
            'year': {'$gte': year_start, '$lte': year_end}
        }).sort('rating.average', -1).limit(n))
    
    def get_popular_genres(self, min_rating: float = 7.0, 
                           min_movies: int = 50) -> List[Dict]:
        """
        Q5: Genres populaires (note moyenne > seuil, plus de N films)
        """
        pipeline = [
            {'$unwind': '$genres'},
            {'$group': {
                '_id': '$genres',
                'count': {'$sum': 1},
                'avg_rating': {'$avg': '$rating.average'}
            }},
            {'$match': {
                'avg_rating': {'$gt': min_rating},
                'count': {'$gt': min_movies}
            }},
            {'$sort': {'avg_rating': -1}}
        ]
        
        return list(self.db.movies_complete.aggregate(pipeline))
    
    def get_similar_movies(self, movie_id: str, limit: int = 5) -> List[Dict]:
        """Récupère des films similaires (même genre ou réalisateur)"""
        movie = self.get_movie_complete(movie_id)
        if not movie:
            return []
        
        genres = movie.get('genres', [])
        
        return list(self.db.movies_complete.find({
            '_id': {'$ne': movie_id},
            'genres': {'$in': genres}
        }).sort('rating.average', -1).limit(limit))
    
    def close(self):
        """Ferme la connexion"""
        if self._client:
            self._client.close()
            self._client = None


# Instance singleton pour faciliter l'import
mongo_service = MongoDBService()
