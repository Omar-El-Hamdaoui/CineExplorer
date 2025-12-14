#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
CineExplorer - IMPORT_DATA.PY (100% COMPLET)
================================================================================
"""

import sqlite3
import pandas as pd
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent.parent.parent / "data" / "imdb.db"
CSV_FOLDER = Path(__file__).parent.parent.parent / "data" / "csv"

# Mapping des colonnes
COLUMN_MAPPING = {
    "('pid',)": "person_id",
    "('primaryName',)": "name",
    "('birthYear',)": "birth_year",
    "('deathYear',)": "death_year",
    "('mid',)": "movie_id",
    "('titleType',)": "title_type",
    "('primaryTitle',)": "primary_title",
    "('originalTitle',)": "original_title",
    "('isAdult',)": "is_adult",
    "('startYear',)": "start_year",
    "('endYear',)": "end_year",
    "('runtimeMinutes',)": "runtime_minutes",
    "('averageRating',)": "average_rating",
    "('numVotes',)": "num_votes",
    "('ordering',)": "ordering",
    "('title',)": "title",
    "('region',)": "region",
    "('language',)": "language",
    "('types',)": "types",
    "('attributes',)": "attributes",
    "('isOriginalTitle',)": "is_original_title",
    "('genre',)": "genre",
    "('jobName',)": "job_name",
    "('category',)": "category",
    "('job',)": "job",
    "('name',)": "name",  # CHARACTERS table uses 'name'
    "('parentMid',)": "parent_movie_id",
    "('seasonNumber',)": "season_number",
    "('episodeNumber',)": "episode_number",
}


def clean_data(table_name, df):
    """Nettoyer les données selon la table"""

    if table_name == 'PERSONS':
        if 'birth_year' in df.columns and 'death_year' in df.columns:
            mask = (df['birth_year'].isna()) | (df['death_year'].isna()) | (df['birth_year'] <= df['death_year'])
            df = df[mask]
        return df

    elif table_name == 'MOVIES':
        if 'runtime_minutes' in df.columns:
            df = df[(df['runtime_minutes'].isna()) | (df['runtime_minutes'] > 0)]
        return df

    elif table_name == 'EPISODES':
        # FIX: episode_number > 0 (pas = 0)
        if 'season_number' in df.columns:
            df = df[(df['season_number'].notna()) & (df['season_number'] > 0)]
        if 'episode_number' in df.columns:
            df = df[(df['episode_number'].notna()) & (df['episode_number'] > 0)]
        return df

    elif table_name == 'PROFESSIONS':
        if 'job_name' in df.columns:
            df = df[df['job_name'].notna()]
        return df

    elif table_name == 'RATINGS':
        if 'average_rating' in df.columns:
            df = df[(df['average_rating'] >= 1.0) & (df['average_rating'] <= 10.0)]
        return df

    elif table_name == 'CHARACTERS':
        # Supprimer les lignes où name est NULL
        if 'name' in df.columns:
            df = df[df['name'].notna()]
        # Supprimer les doublons (même movie_id, person_id, name)
        df = df.drop_duplicates(subset=['movie_id', 'person_id', 'name'], keep='first')
        return df

    return df


def main():
    print(f"\n{'=' * 80}")
    print(f"🎬 CINEEXPLORER - IMPORT IMDB (100% COMPLET)")
    print(f"{'=' * 80}\n")

    if not DB_PATH.exists():
        print(f"❌ Base de données n'existe pas!")
        return 1

    if not CSV_FOLDER.exists():
        print(f"❌ Dossier CSV n'existe pas!")
        return 1

    print(f"{'=' * 80}")
    print(f"📊 IMPORT DES DONNÉES")
    print(f"{'=' * 80}\n")

    tables = [
        ('PERSONS', 'persons.csv'),
        ('MOVIES', 'movies.csv'),
        ('RATINGS', 'ratings.csv'),
        ('TITLES', 'titles.csv'),
        ('EPISODES', 'episodes.csv'),
        ('GENRES', 'genres.csv'),
        ('PROFESSIONS', 'professions.csv'),
        ('PRINCIPALS', 'principals.csv'),
        ('DIRECTORS', 'directors.csv'),
        ('WRITERS', 'writers.csv'),
        ('CHARACTERS', 'characters.csv'),
        ('KNOWNFORMOVIES', 'knownformovies.csv'),
    ]

    for table_name, csv_file in tables:
        csv_path = CSV_FOLDER / csv_file

        if not csv_path.exists():
            print(f"⚠️  {table_name:20s} CSV NOT FOUND")
            continue

        try:
            print(f"⏳ {table_name:20s}", end=" ", flush=True)

            df = pd.read_csv(csv_path)

            if len(df) == 0:
                print(f"⚠️  VIDE")
                continue

            original_count = len(df)

            rename_dict = {}
            for col in df.columns:
                col_str = str(col)
                if col_str in COLUMN_MAPPING:
                    rename_dict[col] = COLUMN_MAPPING[col_str]

            if rename_dict:
                df = df.rename(columns=rename_dict)

            df = clean_data(table_name, df)
            df = df.where(pd.notnull(df), None)

            final_count = len(df)
            skipped = original_count - final_count

            if final_count > 0:
                conn = sqlite3.connect(str(DB_PATH))
                df.to_sql(table_name, conn, if_exists='append', index=False)
                conn.close()

                if skipped > 0:
                    print(f"✅ {final_count:>10,} lignes ({skipped} supprimées)")
                else:
                    print(f"✅ {final_count:>10,} lignes")
            else:
                print(f"⚠️  0 lignes valides")

        except Exception as e:
            print(f"❌ {str(e)[:40]}")

    print(f"\n{'=' * 80}")
    print(f"📈 RÉSULTAT FINAL")
    print(f"{'=' * 80}\n")

    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()

    table_names = [
        'PERSONS', 'MOVIES', 'RATINGS', 'GENRES', 'TITLES',
        'PROFESSIONS', 'PRINCIPALS', 'DIRECTORS', 'WRITERS',
        'CHARACTERS', 'KNOWNFORMOVIES', 'EPISODES'
    ]

    print(f"{'Table':<35} {'Lignes':>15}")
    print("-" * 55)

    db_total = 0
    for table_name in table_names:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            db_total += count
            status = "✅" if count > 0 else "⚠️"
            print(f"{status} {table_name:<33} {count:>15,}")
        except:
            print(f"⚠️  {table_name:<33} {'ERROR':>15}")

    conn.close()

    print("-" * 55)
    print(f"{'TOTAL':<35} {db_total:>15,}")
    print(f"\n{'=' * 80}\n")

    if db_total > 10000000:
        print(f"✅ SUCCÈS COMPLET! {db_total:,} lignes! 🎉\n")

    return 0


if __name__ == '__main__':
    sys.exit(main())