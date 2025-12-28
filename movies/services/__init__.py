"""
CinéExplorer Services Package

Services d'accès aux bases de données:
- sqlite_service: Accès à la base SQLite (Phase 1)
- mongo_service: Accès à MongoDB Replica Set (Phase 2 & 3)
"""

from .sqlite_service import sqlite_service, SQLiteService
from .mongo_service import mongo_service, MongoDBService

__all__ = [
    'sqlite_service', 'SQLiteService',
    'mongo_service', 'MongoDBService',
]
