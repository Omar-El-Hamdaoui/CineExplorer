"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - MongoDB Service                                         ║
║                                                                                ║
║     Phase 4: Service d'accès MongoDB avec Replica Set                         ║
║     Note: Renomme _id en id pour compatibilité Django templates               ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

from django.conf import settings
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
import logging

logger = logging.getLogger(__name__)


def fix_id(doc):
    """Renomme _id en id pour compatibilité Django templates"""
    if doc and '_id' in doc:
        doc['id'] = doc.pop('_id')
    return doc


def fix_ids(docs):
    """Renomme _id en id pour une liste de documents"""
    return [fix_id(doc) for doc in docs]


class MongoDBService:
    """
    Service Singleton pour l'accès à MongoDB
    Gère la connexion au Replica Set et fournit les méthodes d'accès aux données
    """
    
    _instance = None
    _client = None
    _db = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """Initialise la connexion MongoDB"""
        try:
            mongo_config = settings.MONGODB_CONFIG
            self._client = MongoClient(
                mongo_config['URI'],
                **mongo_config.get('OPTIONS', {})
            )
            self._db = self._client[mongo_config['DATABASE_STRUCTURED']]
            logger.info("MongoDB connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {e}")
            self._client = None
            self._db = None
    
    @property
    def client(self):
        return self._client
    
    @property
    def db(self):
        return self._db
    
    def is_connected(self):
        """Vérifie si la connexion est active"""
        try:
            if self._client:
                self._client.admin.command('ping')
                return True
        except (ConnectionFailure, ServerSelectionTimeoutError):
            pass
        return False
    
    def get_replica_status(self):
        """Retourne le statut du Replica Set"""
        try:
            status = self._client.admin.command('replSetGetStatus')
            members = []
            for member in status.get('members', []):
                state_map = {
                    0: 'STARTUP', 1: 'PRIMARY', 2: 'SECONDARY',
                    3: 'RECOVERING', 7: 'ARBITER', 8: 'DOWN'
                }
                members.append({
                    'name': member.get('name'),
                    'state': state_map.get(member.get('state'), 'UNKNOWN'),
                    'health': member.get('health', 0)
                })
            return {
                'set': status.get('set'),
                'members': members
            }
        except Exception as e:
            logger.error(f"Error getting replica status: {e}")
            return {'set': None, 'members': []}
    
    def get_stats(self):
        """Retourne les statistiques de la base de données"""
        try:
            stats = self._db.command('dbStats')
            return {
                'data_size_mb': round(stats.get('dataSize', 0) / (1024 * 1024), 2),
                'storage_size_mb': round(stats.get('storageSize', 0) / (1024 * 1024), 2),
                'collections_count': stats.get('collections', 0),
                'movies_count': self._db.movies.count_documents({}),
                'movies_complete_count': self._db.movies_complete.count_documents({}),
                'persons_count': self._db.persons.count_documents({})
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}
    
    # =========================================================================
    # FILMS - Collection movies_complete
    # =========================================================================
    
    def get_movie_complete(self, movie_id):
        """Récupère un film complet par son ID"""
        try:
            movie = self._db.movies_complete.find_one({'_id': movie_id})
            return fix_id(movie)
        except Exception as e:
            logger.error(f"Error getting movie {movie_id}: {e}")
            return None
    
    def get_movies_complete(self, limit=20, skip=0, genre=None, year_min=None, 
                           year_max=None, min_rating=None, sort_by='title', sort_order=1):
        """Récupère une liste de films avec filtres"""
        try:
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
            
            # Mapping du tri
            sort_field_map = {
                'title': 'title',
                'year': 'year',
                'rating': 'rating.average',
                'votes': 'rating.votes'
            }
            sort_field = sort_field_map.get(sort_by, 'title')
            
            cursor = self._db.movies_complete.find(query)\
                .sort(sort_field, sort_order)\
                .skip(skip)\
                .limit(limit)
            
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error getting movies: {e}")
            return []
    
    def count_movies_complete(self, genre=None, year_min=None, year_max=None, min_rating=None):
        """Compte le nombre de films avec filtres"""
        try:
            query = {}
            
            if genre:
                query['genres'] = genre
            if year_min:
                query['year'] = query.get('year', {})
                query['year']['$gte'] = year_min
            if year_max:
                query['year'] = query.get('year', {})
                query['year']['$lte'] = year_max
            if min_rating:
                query['rating.average'] = {'$gte': min_rating}
            
            return self._db.movies_complete.count_documents(query)
        except Exception as e:
            logger.error(f"Error counting movies: {e}")
            return 0
    
    def get_top_movies(self, limit=10, min_votes=10000):
        """Récupère les meilleurs films"""
        try:
            cursor = self._db.movies_complete.find({
                'rating.votes': {'$gte': min_votes}
            }).sort('rating.average', -1).limit(limit)
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error getting top movies: {e}")
            return []
    
    def search_movies(self, query, limit=20):
        """Recherche de films par titre"""
        try:
            cursor = self._db.movies_complete.find({
                'title': {'$regex': query, '$options': 'i'}
            }).limit(limit)
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error searching movies: {e}")
            return []
    
    def get_similar_movies(self, movie_id, limit=6):
        """Récupère des films similaires basés sur le genre"""
        try:
            movie = self._db.movies_complete.find_one({'_id': movie_id})
            if not movie or 'genres' not in movie or not movie['genres']:
                return []
            
            cursor = self._db.movies_complete.find({
                '_id': {'$ne': movie_id},
                'genres': {'$in': movie['genres']},
                'rating.votes': {'$gte': 10000}
            }).sort('rating.average', -1).limit(limit)
            
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error getting similar movies: {e}")
            return []
    
    # =========================================================================
    # GENRES
    # =========================================================================
    
    def get_all_genres(self):
        """Récupère tous les genres distincts"""
        try:
            return self._db.movies_complete.distinct('genres')
        except Exception as e:
            logger.error(f"Error getting genres: {e}")
            return []
    
    def get_genres_stats(self):
        """Récupère les statistiques par genre"""
        try:
            pipeline = [
                {'$unwind': '$genres'},
                {'$group': {
                    '_id': '$genres',
                    'count': {'$sum': 1},
                    'avg_rating': {'$avg': '$rating.average'}
                }},
                {'$sort': {'count': -1}},
                {'$project': {
                    'genre': '$_id',
                    'count': 1,
                    'avg_rating': {'$round': ['$avg_rating', 2]},
                    '_id': 0
                }}
            ]
            return list(self._db.movies_complete.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting genres stats: {e}")
            return []
    
    # =========================================================================
    # PERSONNES
    # =========================================================================
    
    def get_person(self, person_id):
        """Récupère une personne par son ID"""
        try:
            person = self._db.persons.find_one({'_id': person_id})
            return fix_id(person)
        except Exception as e:
            logger.error(f"Error getting person {person_id}: {e}")
            return None
    
    def search_persons(self, query, limit=20):
        """Recherche de personnes par nom"""
        try:
            cursor = self._db.persons.find({
                'name': {'$regex': query, '$options': 'i'}
            }).limit(limit)
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error searching persons: {e}")
            return []
    
    def get_actor_filmography(self, actor_name, limit=50):
        """Récupère la filmographie d'un acteur"""
        try:
            cursor = self._db.movies_complete.find({
                'cast.name': {'$regex': actor_name, '$options': 'i'}
            }).sort('year', -1).limit(limit)
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error getting filmography: {e}")
            return []
    
    # =========================================================================
    # STATISTIQUES AVANCÉES
    # =========================================================================
    
    def get_movies_by_decade(self):
        """Récupère le nombre de films par décennie"""
        try:
            pipeline = [
                {'$match': {'year': {'$exists': True, '$ne': None}}},
                {'$group': {
                    '_id': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]},
                    'count': {'$sum': 1}
                }},
                {'$sort': {'_id': 1}},
                {'$project': {
                    'decade': '$_id',
                    'count': 1,
                    '_id': 0
                }}
            ]
            return list(self._db.movies_complete.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting movies by decade: {e}")
            return []
    
    def get_rating_distribution(self):
        """Récupère la distribution des notes"""
        try:
            pipeline = [
                {'$match': {'rating.average': {'$exists': True}}},
                {'$group': {
                    '_id': {'$round': ['$rating.average', 0]},
                    'count': {'$sum': 1}
                }},
                {'$sort': {'_id': 1}},
                {'$project': {
                    'rating': '$_id',
                    'count': 1,
                    '_id': 0
                }}
            ]
            return list(self._db.movies_complete.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting rating distribution: {e}")
            return []
    
    def get_top_directors(self, limit=10):
        """Récupère les réalisateurs les plus prolifiques"""
        try:
            pipeline = [
                {'$unwind': '$directors'},
                {'$group': {
                    '_id': '$directors.name',
                    'movie_count': {'$sum': 1},
                    'avg_rating': {'$avg': '$rating.average'}
                }},
                {'$match': {'movie_count': {'$gte': 3}}},
                {'$sort': {'movie_count': -1}},
                {'$limit': limit},
                {'$project': {
                    'name': '$_id',
                    'movie_count': 1,
                    'avg_rating': {'$round': ['$avg_rating', 2]},
                    '_id': 0
                }}
            ]
            return list(self._db.movies_complete.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting top directors: {e}")
            return []
    
    def get_top_actors(self, limit=10):
        """Récupère les acteurs les plus prolifiques"""
        try:
            pipeline = [
                {'$unwind': '$cast'},
                {'$group': {
                    '_id': '$cast.name',
                    'movie_count': {'$sum': 1},
                    'avg_rating': {'$avg': '$rating.average'}
                }},
                {'$match': {'movie_count': {'$gte': 5}}},
                {'$sort': {'movie_count': -1}},
                {'$limit': limit},
                {'$project': {
                    'name': '$_id',
                    'movie_count': 1,
                    'avg_rating': {'$round': ['$avg_rating', 2]},
                    '_id': 0
                }}
            ]
            return list(self._db.movies_complete.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting top actors: {e}")
            return []
    
    def get_top_n_movies_by_genre(self, genre, year_min=None, year_max=None, n=10):
        """Récupère les N meilleurs films d'un genre"""
        try:
            query = {'genres': genre}
            if year_min or year_max:
                query['year'] = {}
                if year_min:
                    query['year']['$gte'] = year_min
                if year_max:
                    query['year']['$lte'] = year_max
            
            cursor = self._db.movies_complete.find(query)\
                .sort('rating.average', -1)\
                .limit(n)
            
            return fix_ids(list(cursor))
        except Exception as e:
            logger.error(f"Error getting top movies by genre: {e}")
            return []
    
    def get_popular_genres(self, min_rating=7.0, min_count=50):
        """Récupère les genres populaires"""
        try:
            pipeline = [
                {'$unwind': '$genres'},
                {'$group': {
                    '_id': '$genres',
                    'count': {'$sum': 1},
                    'avg_rating': {'$avg': '$rating.average'}
                }},
                {'$match': {
                    'avg_rating': {'$gte': min_rating},
                    'count': {'$gte': min_count}
                }},
                {'$sort': {'avg_rating': -1}},
                {'$project': {
                    'genre': '$_id',
                    'count': 1,
                    'avg_rating': {'$round': ['$avg_rating', 2]},
                    '_id': 0
                }}
            ]
            return list(self._db.movies_complete.aggregate(pipeline))
        except Exception as e:
            logger.error(f"Error getting popular genres: {e}")
            return []


# Instance singleton pour utilisation globale
mongo_service = MongoDBService()
