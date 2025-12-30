# 🎬 CinéExplorer

[![Python](https://img.shields.io/badge/Python-3.12-blue.svg)](https://www.python.org/)
[![Django](https://img.shields.io/badge/Django-6.0-green.svg)](https://www.djangoproject.com/)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-brightgreen.svg)](https://www.mongodb.com/)
[![SQLite](https://img.shields.io/badge/SQLite-3-lightgrey.svg)](https://www.sqlite.org/)
[![Bootstrap](https://img.shields.io/badge/Bootstrap-5.3-purple.svg)](https://getbootstrap.com/)

> **Plateforme Web de Découverte de Films** - Projet Bases de Données Avancées  
> Aix-Marseille Université - Polytech Marseille - 2025

---

## 📋 Table des matières

- [À propos](#-à-propos)
- [Fonctionnalités](#-fonctionnalités)
- [Architecture](#-architecture)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Utilisation](#-utilisation)
- [Structure du projet](#-structure-du-projet)
- [Benchmarks](#-benchmarks)
- [Auteur](#-auteur)

---

## 🎯 À propos

CinéExplorer est une application web permettant d'explorer une base de données IMDB de **291 234 films** et **632 323 personnes**. Le projet démontre l'utilisation combinée de bases de données relationnelles (SQLite) et NoSQL (MongoDB) avec une interface Django moderne.

### Phases du projet

| Phase | Description | Status |
|-------|-------------|--------|
| **Phase 1** | Base SQLite - 12 tables normalisées (3NF) | ✅ Complet |
| **Phase 2** | Migration MongoDB - Documents structurés | ✅ Complet |
| **Phase 3** | Replica Set - 3 nœuds haute disponibilité | ✅ Complet |
| **Phase 4** | Interface Django - 5 pages responsive | ✅ Complet |

---

## ✨ Fonctionnalités

### Pages Web

| Page | URL | Description |
|------|-----|-------------|
| 🏠 **Accueil** | `/` | Statistiques, Top 10 films, Status des bases |
| 🎬 **Films** | `/movies/` | Liste avec pagination, filtres et tri |
| 📄 **Détail** | `/movies/<id>/` | Informations complètes, casting, films similaires |
| 🔍 **Recherche** | `/search/` | Recherche films et personnes |
| 📊 **Statistiques** | `/stats/` | Graphiques interactifs (Chart.js) |

### Fonctionnalités clés

- ✅ **Pagination** : 20 films par page
- ✅ **Filtres** : Genre, Année (min/max), Note minimale
- ✅ **Tri** : Titre, Année, Note, Popularité (ASC/DESC)
- ✅ **Responsive** : Design mobile-first (Bootstrap 5)
- ✅ **Graphiques** : Distribution par genre, décennie, notes
- ✅ **Haute disponibilité** : MongoDB Replica Set (3 nœuds)

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   COUCHE PRÉSENTATION                    │
│              Django 6.0 + Bootstrap 5                    │
│         Templates HTML │ Chart.js │ Responsive           │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                    COUCHE SERVICES                       │
│            SQLiteService │ MongoDBService                │
│           Abstraction des accès aux données              │
└─────────────────────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
┌─────────────────────────┐   ┌─────────────────────────┐
│        SQLite           │   │   MongoDB Replica Set   │
│       imdb.db           │   │         rs0             │
│       786 MB            │   │  ┌─────┬─────┬─────┐    │
│                         │   │  │27017│27018│27019│    │
│  • Listes & Filtres     │   │  │ PRI │ SEC │ SEC │    │
│  • Statistiques SQL     │   │  └─────┴─────┴─────┘    │
│  • Recherche            │   │                         │
└─────────────────────────┘   │  • Détails films        │
                              │  • Documents complets   │
                              └─────────────────────────┘
```

### Stratégie Multi-Bases

| Opération | Base | Justification |
|-----------|------|---------------|
| Liste films + filtres | MongoDB | Documents pré-agrégés |
| Détail complet film | MongoDB | 1 document = toutes les infos |
| Statistiques globales | SQLite | Agrégations SQL optimisées |
| Recherche textuelle | MongoDB | Index text, regex |

---

## 📦 Prérequis

- **Python** 3.10+
- **MongoDB** 7.0+ (Community Edition)
- **pip** (gestionnaire de paquets Python)
- **Git**

---

## 🚀 Installation

### 1. Cloner le projet

```bash
git clone https://github.com/votre-username/cineexplorer.git
cd cineexplorer
```

### 2. Créer l'environnement virtuel

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# ou
.venv\Scripts\activate     # Windows
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Démarrer le Replica Set MongoDB

```bash
# Créer les répertoires
mkdir -p data/mongo/db-1 data/mongo/db-2 data/mongo/db-3

# Terminal 1 - Primary
mongod --replSet rs0 --port 27017 --dbpath ./data/mongo/db-1 --bind_ip localhost

# Terminal 2 - Secondary 1
mongod --replSet rs0 --port 27018 --dbpath ./data/mongo/db-2 --bind_ip localhost

# Terminal 3 - Secondary 2
mongod --replSet rs0 --port 27019 --dbpath ./data/mongo/db-3 --bind_ip localhost
```

Ou utiliser le script fourni :

```bash
./scripts/phase3_replica/setup_replica.sh start
```

### 5. Initialiser le Replica Set (première fois)

```bash
mongosh --port 27017 --eval '
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "localhost:27017" },
    { _id: 1, host: "localhost:27018" },
    { _id: 2, host: "localhost:27019" }
  ]
})'
```

### 6. Importer les données

```bash
# SQLite (Phase 1)
python scripts/phase1_sqlite/import_data.py

# MongoDB (Phase 2)
python scripts/phase2_mongodb/migrate_flat.py
python scripts/phase2_mongodb/build_movies_complete.py
```

### 7. Créer les index MongoDB (optimisation)

```bash
python scripts/phase2_mongodb/create_indexes.py
```

### 8. Lancer l'application

```bash
python manage.py runserver
```

Ouvrir http://127.0.0.1:8000 dans votre navigateur.

---

## 💻 Utilisation

### Commandes utiles

| Commande | Description |
|----------|-------------|
| `python manage.py runserver` | Démarrer le serveur Django |
| `./scripts/phase3_replica/setup_replica.sh start` | Démarrer le Replica Set |
| `./scripts/phase3_replica/setup_replica.sh stop` | Arrêter le Replica Set |
| `./scripts/phase3_replica/setup_replica.sh status` | Status du Replica Set |
| `python scripts/phase2_mongodb/create_indexes.py` | Créer les index MongoDB |
| `python scripts/benchmark_phase4.py` | Exécuter les benchmarks |

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/test/` | Test des connexions (JSON) |
| `/api/genres/` | Liste des genres (JSON) |
| `/api/stats/` | Statistiques (JSON) |

---

## 📁 Structure du projet

```
cineexplorer/
├── config/                     # Configuration Django
│   ├── settings.py
│   ├── urls.py
│   └── wsgi.py
│
├── movies/                     # Application principale
│   ├── services/
│   │   ├── sqlite_service.py   # Accès SQLite
│   │   └── mongo_service.py    # Accès MongoDB
│   ├── templates/movies/
│   │   ├── base.html           # Template de base
│   │   ├── home.html           # Page d'accueil
│   │   ├── list.html           # Liste des films
│   │   ├── detail.html         # Détail d'un film
│   │   ├── search.html         # Recherche
│   │   └── stats.html          # Statistiques
│   ├── views.py
│   └── urls.py
│
├── scripts/
│   ├── phase1_sqlite/          # Scripts Phase 1
│   ├── phase2_mongodb/         # Scripts Phase 2
│   │   └── create_indexes.py   # Création des index
│   ├── phase3_replica/         # Scripts Phase 3
│   │   └── setup_replica.sh
│   └── benchmark_phase4.py     # Benchmarks
│
├── data/
│   ├── csv/                    # Fichiers IMDB originaux
│   ├── imdb.db                 # Base SQLite
│   └── mongo/                  # Données MongoDB
│       ├── db-1/               # Nœud 1 (Primary)
│       ├── db-2/               # Nœud 2 (Secondary)
│       └── db-3/               # Nœud 3 (Secondary)
│
├── reports/                    # Rapports PDF
│   ├── livrable1/
│   ├── livrable2/
│   ├── livrable3/
│   └── final/
│
├── requirements.txt
├── README.md
└── .gitignore
```

---

## 📊 Benchmarks

Résultats des tests de performance (moyenne sur 5 itérations) :

| Opération | SQLite (ms) | MongoDB (ms) | Gagnant | Ratio |
|-----------|-------------|--------------|---------|-------|
| Stats générales | 126.29 | 275.64 | SQLite | 2.2x |
| Liste films (20) | 166.85 | **0.57** | **MongoDB** | **292x** |
| Liste filtrée | 118.44 | **0.72** | **MongoDB** | **164x** |
| Détail film | **0.34** | 1.40 | SQLite | 4.1x |
| Recherche | **20.51** | 64.35 | SQLite | 3.1x |
| Stats par genre | 424.76 | **296.97** | **MongoDB** | 1.4x |
| Films/décennie | **120.53** | 299.28 | SQLite | 2.5x |

**Conclusion** : La stratégie multi-bases est validée. MongoDB excelle pour les listes (292x plus rapide), SQLite pour les requêtes simples.

---

## 🛠 Technologies

| Catégorie | Technologie | Version |
|-----------|-------------|---------|
| **Backend** | Django | 6.0 |
| **Base SQL** | SQLite | 3.x |
| **Base NoSQL** | MongoDB | 7.0 |
| **Frontend** | Bootstrap | 5.3 |
| **Graphiques** | Chart.js | 4.x |
| **Python** | Python | 3.12 |
| **Driver MongoDB** | PyMongo | 4.x |

---

## 📝 Livrables

- ✅ **Livrable 1** : Exploration et SQLite (Phase 1)
- ✅ **Livrable 2** : Migration MongoDB (Phase 2)
- ✅ **Livrable 3** : Replica Set (Phase 3)
- ✅ **Livrable 4** : Projet final Django (Phase 4)

---

## 👤 Auteur

**Projet réalisé dans le cadre du module 4A-BDA**  
Bases de Données Avancées - Aix-Marseille Université  
Polytech Marseille - Département Informatique

---

## 📄 Licence

Ce projet est réalisé dans un cadre éducatif.  
Les données IMDB sont utilisées conformément aux conditions d'utilisation d'IMDB.

---

<p align="center">
  <b>CinéExplorer</b> - Décembre 2025<br>
  <i>Projet Bases de Données Avancées</i>
</p>                                                