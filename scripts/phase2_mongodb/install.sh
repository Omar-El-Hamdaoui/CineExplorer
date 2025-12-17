#!/bin/bash
# -*- coding: utf-8 -*-

# ============================================================================
# 🎬 CINEEXPLORER - T2.1 INSTALLATION SCRIPT CORRIGÉ (SANS AUTH)
# ============================================================================

set -e

# Couleurs
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

# ============================================================================
# FONCTIONS
# ============================================================================

print_header() {
    echo -e "\n${BLUE}${BOLD}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}${BOLD}║${NC} $1"
    echo -e "${BLUE}${BOLD}╚════════════════════════════════════════════════════════════════╝${NC}\n"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# ============================================================================
# ÉTAPE 1: VÉRIFIER LA CONFIGURATION
# ============================================================================

check_system() {
    print_header "ÉTAPE 1: Vérification du système"

    echo "Système d'exploitation:"
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        print_success "Linux détecté"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        print_success "macOS détecté"
    else
        print_warning "Système: $OSTYPE"
    fi

    echo -e "\nVersion Python:"
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version)
        print_success "$PYTHON_VERSION"
    else
        print_error "Python 3 non trouvé!"
        return 1
    fi

    echo -e "\nVérification de pip:"
    if command -v pip &> /dev/null || command -v pip3 &> /dev/null; then
        print_success "pip trouvé"
    else
        print_error "pip non trouvé!"
        return 1
    fi

    return 0
}

# ============================================================================
# ÉTAPE 2: VÉRIFIER DOCKER
# ============================================================================

check_docker() {
    print_header "ÉTAPE 2: Vérification de Docker"

    if command -v docker &> /dev/null; then
        DOCKER_VERSION=$(docker --version)
        print_success "$DOCKER_VERSION"

        if docker ps &> /dev/null; then
            print_success "Docker daemon est en cours d'exécution"
            return 0
        else
            print_warning "Docker est installé mais le daemon n'est pas en cours d'exécution"
            return 1
        fi
    else
        print_error "Docker n'est pas installé"
        echo "Installez Docker: https://docs.docker.com/get-docker/"
        return 1
    fi
}

# ============================================================================
# ÉTAPE 3: NETTOYER ANCIEN CONTAINER (SI PRÉSENT)
# ============================================================================

cleanup_old_mongodb() {
    print_header "ÉTAPE 3: Nettoyage (ancien MongoDB si présent)"

    if docker ps -a --format '{{.Names}}' | grep -q '^mongodb$'; then
        print_warning "Container 'mongodb' existant trouvé"
        echo "Arrêt et suppression..."

        docker stop mongodb 2>/dev/null || true
        docker rm mongodb 2>/dev/null || true
        sleep 1

        print_success "Ancien container supprimé"
    else
        print_success "Aucun ancien container trouvé"
    fi
}

# ============================================================================
# ÉTAPE 4: LANCER MONGODB SANS AUTHENTIFICATION
# ============================================================================

launch_mongodb() {
    print_header "ÉTAPE 4: Lancer MongoDB avec Docker (SANS authentification)"

    echo "Création et lancement du container MongoDB..."

    docker run -d \
        --name mongodb \
        -p 27017:27017 \
        -v mongodb_data:/data/db \
        mongo:7.0

    echo "⏳ Attente du démarrage de MongoDB (5 secondes)..."
    sleep 5

    if docker ps --format '{{.Names}}' | grep -q '^mongodb$'; then
        print_success "MongoDB est lancé!"
        echo ""
        echo "Infos du container:"
        docker ps --filter "name=mongodb" --format "  ID: {{.ID}}\n  Image: {{.Image}}\n  Status: {{.Status}}"
        return 0
    else
        print_error "Échec du lancement de MongoDB"
        echo "Logs:"
        docker logs mongodb 2>&1 | tail -20
        return 1
    fi
}

# ============================================================================
# ÉTAPE 5: INSTALLER PYMONGO
# ============================================================================

install_pymongo() {
    print_header "ÉTAPE 5: Installer PyMongo"

    echo "Installation de PyMongo..."

    if pip install -q pymongo 2>/dev/null || pip3 install -q pymongo 2>/dev/null; then
        PYMONGO_VERSION=$(python3 -c "import pymongo; print(pymongo.__version__)" 2>/dev/null)
        print_success "PyMongo installé (version $PYMONGO_VERSION)"
        return 0
    else
        print_error "Échec de l'installation de PyMongo"
        return 1
    fi
}

# ============================================================================
# ÉTAPE 6: TESTER LA CONNEXION
# ============================================================================

test_connection() {
    print_header "ÉTAPE 6: Tester la connexion MongoDB"

    python3 << 'PYTHON_TEST'
import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

print("⏳ Connexion à MongoDB (sans authentification)...")

try:
    client = MongoClient(
        "mongodb://localhost:27017",
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000
    )

    # Test ping
    result = client.admin.command('ping')
    print(f"✅ Connexion réussie!")
    print(f"   Réponse du serveur: {result}")

    # Lister les bases
    dbs = client.list_database_names()
    print(f"\n✅ Bases disponibles ({len(dbs)}):")
    for db in dbs:
        print(f"   • {db}")

    # Vérifier la version du serveur
    server_info = client.server_info()
    print(f"\n✅ Infos serveur:")
    print(f"   Version MongoDB: {server_info.get('version', 'N/A')}")

    # Test insertion/récupération
    print(f"\n✅ Test insertion/récupération:")
    test_db = client["test_cineexplorer"]
    test_coll = test_db["test"]
    test_coll.insert_one({"test": "OK"})
    doc = test_coll.find_one({"test": "OK"})
    print(f"   Document inséré et récupéré: {doc}")
    test_coll.drop()

    client.close()
    print(f"\n✅ Tous les tests réussis!")
    sys.exit(0)

except (ConnectionFailure, ServerSelectionTimeoutError) as e:
    print(f"❌ Erreur de connexion: {e}")
    print(f"\n⚠️  MongoDB n'est pas accessible à mongodb://localhost:27017")
    sys.exit(1)

except Exception as e:
    print(f"❌ Erreur: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
PYTHON_TEST

    return $?
}

# ============================================================================
# MAIN
# ============================================================================

main() {
    echo -e "\n"
    echo -e "${BLUE}${BOLD}"
    echo "╔════════════════════════════════════════════════════════════════╗"
    echo "║                                                                ║"
    echo "║  🎬 CINEEXPLORER - T2.1 INSTALLATION MONGODB                  ║"
    echo "║     Script corrigé (SANS authentification)                    ║"
    echo "║                                                                ║"
    echo "╚════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}\n"

    if ! check_system; then
        print_error "Problème détecté lors de la vérification du système"
        return 1
    fi

    if ! check_docker; then
        print_error "Docker n'est pas disponible"
        return 1
    fi

    cleanup_old_mongodb

    if ! launch_mongodb; then
        print_error "Impossible de lancer MongoDB"
        return 1
    fi

    if ! install_pymongo; then
        print_error "Impossible d'installer PyMongo"
        return 1
    fi

    if ! test_connection; then
        print_error "Test de connexion échoué"
        return 1
    fi

    # Succès!
    echo ""
    print_header "✅ T2.1 COMPLET!"

    echo "Récapitulatif:"
    print_success "MongoDB lancé (SANS authentification)"
    print_success "PyMongo installé"
    print_success "Connexion testée avec succès"

    echo ""
    echo "Commandes utiles:"
    echo "  • Vérifier MongoDB: docker ps"
    echo "  • Logs MongoDB: docker logs mongodb"
    echo "  • Arrêter: docker stop mongodb"
    echo "  • Redémarrer: docker start mongodb"
    echo ""
    echo "Prochaines étapes:"
    echo "  • T2.2 - Migration des collections plates"
    echo ""

    return 0
}

# ============================================================================
# EXÉCUTION
# ============================================================================

main
exit $?