#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script pour vider toutes les tables de la base de données
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "imdb.db"

print("\n" + "="*80)
print("🗑️  SUPPRESSION DE TOUTES LES DONNÉES")
print("="*80 + "\n")

print(f"📁 Base: {DB_PATH}\n")

if not DB_PATH.exists():
    print("❌ Base de données n'existe pas!")
    exit(1)

conn = sqlite3.connect(str(DB_PATH))
cursor = conn.cursor()

# Désactiver les contraintes FK temporairement
cursor.execute("PRAGMA foreign_keys = OFF")

# Liste des tables
tables = [
    'CHARACTERS',
    'DIRECTORS',
    'WRITERS',
    'PRINCIPALS',
    'PROFESSIONS',
    'GENRES',
    'EPISODES',
    'TITLES',
    'RATINGS',
    'KNOWNFORMOVIES',
    'MOVIES',
    'PERSONS'
]

# Supprimer les données de chaque table
for table in tables:
    try:
        cursor.execute(f"DELETE FROM {table}")
        print(f"✅ {table:20s} vidée")
    except Exception as e:
        print(f"⚠️  {table:20s} erreur: {e}")

conn.commit()

# Réactiver les contraintes FK
cursor.execute("PRAGMA foreign_keys = ON")
conn.close()

print("\n" + "="*80)
print("✅ BASE DE DONNÉES VIDÉE!")
print("="*80 + "\n")
print("Vous pouvez maintenant relancer l'import!\n")