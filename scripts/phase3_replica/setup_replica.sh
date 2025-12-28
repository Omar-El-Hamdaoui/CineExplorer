#!/bin/bash

#===============================================================================
# Script : setup_replica.sh
# Description : Configuration d'un Replica Set MongoDB (3 nœuds)
# Projet : CinéExplorer - Phase 3
# Usage : ./setup_replica.sh [start|stop|status|init|clean]
#===============================================================================

# Configuration
REPLICA_SET_NAME="rs0"

# Obtenir le chemin absolu du répertoire du script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Remonter à la racine du projet (scripts/phase3_replica -> racine)
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BASE_DATA_DIR="${PROJECT_ROOT}/data/mongo"

PORTS=(27017 27018 27019)
DB_DIRS=("db-1" "db-2" "db-3")

# Couleurs pour les messages
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

#-------------------------------------------------------------------------------
# Fonctions utilitaires
#-------------------------------------------------------------------------------

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo ""
    echo "=============================================="
    echo -e "${BLUE}$1${NC}"
    echo "=============================================="
}

#-------------------------------------------------------------------------------
# Création des répertoires de données
#-------------------------------------------------------------------------------

create_directories() {
    print_header "Création des répertoires de données"

    print_info "Racine du projet : $PROJECT_ROOT"
    print_info "Répertoire données : $BASE_DATA_DIR"

    for dir in "${DB_DIRS[@]}"; do
        local full_path="${BASE_DATA_DIR}/${dir}"
        if [ ! -d "$full_path" ]; then
            mkdir -p "$full_path"
            print_success "Répertoire créé : $full_path"
        else
            print_info "Répertoire existe déjà : $full_path"
        fi
    done
}

#-------------------------------------------------------------------------------
# Nettoyage des fichiers lock
#-------------------------------------------------------------------------------

clean_locks() {
    print_info "Nettoyage des fichiers lock..."

    for dir in "${DB_DIRS[@]}"; do
        local lock_file="${BASE_DATA_DIR}/${dir}/mongod.lock"
        if [ -f "$lock_file" ]; then
            # Vérifier si le fichier lock est vide ou si le processus n'existe plus
            local pid=$(cat "$lock_file" 2>/dev/null)
            if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
                rm -f "$lock_file"
                print_info "Lock supprimé : $lock_file"
            fi
        fi
    done
}

#-------------------------------------------------------------------------------
# Démarrage des instances MongoDB
#-------------------------------------------------------------------------------

start_instances() {
    print_header "Démarrage des instances MongoDB"

    # Nettoyer les locks orphelins
    clean_locks

    for i in "${!PORTS[@]}"; do
        local port=${PORTS[$i]}
        local db_dir=${DB_DIRS[$i]}
        local db_path="${BASE_DATA_DIR}/${db_dir}"
        local log_file="${db_path}/mongod.log"
        local pid_file="${db_path}/mongod.pid"

        # Vérifier si l'instance est déjà en cours d'exécution
        if [ -f "$pid_file" ] && kill -0 $(cat "$pid_file") 2>/dev/null; then
            print_warning "Instance sur le port $port déjà en cours d'exécution"
            continue
        fi

        # Vérifier si le port est déjà utilisé
        if lsof -i :$port &>/dev/null; then
            print_warning "Port $port déjà utilisé. Tentative d'arrêt..."
            local existing_pid=$(lsof -t -i :$port)
            if [ -n "$existing_pid" ]; then
                kill "$existing_pid" 2>/dev/null
                sleep 2
            fi
        fi

        # Démarrer mongod avec chemins absolus
        print_info "Démarrage de mongod sur le port $port..."
        print_info "  dbpath: $db_path"

        mongod \
            --replSet "$REPLICA_SET_NAME" \
            --port "$port" \
            --dbpath "$db_path" \
            --bind_ip localhost \
            --logpath "$log_file" \
            --logappend \
            --pidfilepath "$pid_file" \
            --fork

        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            sleep 1
            if [ -f "$pid_file" ]; then
                print_success "Instance démarrée sur le port $port (PID: $(cat $pid_file))"
            else
                print_success "Instance démarrée sur le port $port"
            fi
        else
            print_error "Échec du démarrage sur le port $port (code: $exit_code)"
            print_info "Consultez le log : $log_file"
            print_info "Dernières lignes du log :"
            tail -5 "$log_file" 2>/dev/null
        fi
    done

    # Attendre que les instances soient prêtes
    print_info "Attente de 3 secondes pour que les instances soient prêtes..."
    sleep 3
}

#-------------------------------------------------------------------------------
# Arrêt des instances MongoDB
#-------------------------------------------------------------------------------

stop_instances() {
    print_header "Arrêt des instances MongoDB"

    for i in "${!PORTS[@]}"; do
        local port=${PORTS[$i]}
        local db_dir=${DB_DIRS[$i]}
        local db_path="${BASE_DATA_DIR}/${db_dir}"
        local pid_file="${db_path}/mongod.pid"

        # Essayer d'arrêter via le fichier PID
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                print_info "Arrêt de l'instance sur le port $port (PID: $pid)..."
                kill "$pid"
                sleep 2

                # Forcer l'arrêt si nécessaire
                if kill -0 "$pid" 2>/dev/null; then
                    print_warning "Arrêt forcé de l'instance sur le port $port..."
                    kill -9 "$pid" 2>/dev/null
                    sleep 1
                fi

                print_success "Instance sur le port $port arrêtée"
            fi
            rm -f "$pid_file"
        fi

        # Vérifier aussi via le port
        local existing_pid=$(lsof -t -i :$port 2>/dev/null)
        if [ -n "$existing_pid" ]; then
            print_info "Arrêt du processus sur le port $port (PID: $existing_pid)..."
            kill "$existing_pid" 2>/dev/null
            sleep 1
            kill -9 "$existing_pid" 2>/dev/null
            print_success "Processus sur le port $port arrêté"
        fi

        # Nettoyer le fichier lock
        local lock_file="${db_path}/mongod.lock"
        if [ -f "$lock_file" ]; then
            rm -f "$lock_file"
        fi
    done
}

#-------------------------------------------------------------------------------
# Vérification du statut des instances
#-------------------------------------------------------------------------------

check_status() {
    print_header "Statut des instances MongoDB"

    local all_running=true

    for i in "${!PORTS[@]}"; do
        local port=${PORTS[$i]}
        local db_dir=${DB_DIRS[$i]}
        local db_path="${BASE_DATA_DIR}/${db_dir}"
        local pid_file="${db_path}/mongod.pid"

        # Vérifier via le port
        if lsof -i :$port &>/dev/null; then
            local pid=$(lsof -t -i :$port)
            print_success "Port $port : EN COURS (PID: $pid)"
        else
            print_error "Port $port : ARRÊTÉ"
            all_running=false
        fi
    done

    echo ""

    # Vérifier le statut du Replica Set si possible
    if $all_running; then
        print_info "Vérification du Replica Set..."
        mongosh --port 27017 --quiet --eval "
            try {
                const status = rs.status();
                print('Replica Set: ' + status.set);
                print('Membres:');
                status.members.forEach(m => {
                    print('  - ' + m.name + ' : ' + m.stateStr);
                });
            } catch(e) {
                print('Replica Set non initialisé ou erreur: ' + e.message);
            }
        " 2>/dev/null
    fi
}

#-------------------------------------------------------------------------------
# Initialisation du Replica Set
#-------------------------------------------------------------------------------

init_replica_set() {
    print_header "Initialisation du Replica Set"

    # Vérifier que toutes les instances sont en cours
    for port in "${PORTS[@]}"; do
        if ! mongosh --port "$port" --quiet --eval "db.runCommand({ping: 1})" &>/dev/null; then
            print_error "L'instance sur le port $port n'est pas accessible"
            print_info "Lancez d'abord : $0 start"
            return 1
        fi
    done

    print_success "Toutes les instances sont accessibles"
    print_info "Initialisation du Replica Set '$REPLICA_SET_NAME'..."

    mongosh --port 27017 --quiet --eval "
        rs.initiate({
            _id: '${REPLICA_SET_NAME}',
            members: [
                { _id: 0, host: 'localhost:27017' },
                { _id: 1, host: 'localhost:27018' },
                { _id: 2, host: 'localhost:27019' }
            ]
        })
    "

    if [ $? -eq 0 ]; then
        print_success "Commande d'initialisation envoyée"

        # Attendre que l'élection soit terminée
        print_info "Attente de l'élection du Primary (10 secondes)..."
        sleep 10

        # Afficher le statut
        check_status
    else
        print_error "Échec de l'initialisation du Replica Set"
    fi
}

#-------------------------------------------------------------------------------
# Nettoyage complet (suppression des données)
#-------------------------------------------------------------------------------

clean_all() {
    print_header "Nettoyage complet"

    read -p "⚠️  Cette action supprimera toutes les données MongoDB. Continuer ? (o/N) " confirm

    if [[ "$confirm" =~ ^[oOyY]$ ]]; then
        # Arrêter les instances d'abord
        stop_instances

        # Supprimer les données
        for dir in "${DB_DIRS[@]}"; do
            local full_path="${BASE_DATA_DIR}/${dir}"
            if [ -d "$full_path" ]; then
                rm -rf "$full_path"
                print_success "Supprimé : $full_path"
            fi
        done

        print_success "Nettoyage terminé"
    else
        print_info "Annulé"
    fi
}

#-------------------------------------------------------------------------------
# Affichage de l'aide
#-------------------------------------------------------------------------------

show_help() {
    echo ""
    echo "Usage: $0 [COMMANDE]"
    echo ""
    echo "Commandes disponibles:"
    echo "  start     Démarrer les 3 instances MongoDB"
    echo "  stop      Arrêter toutes les instances"
    echo "  status    Afficher le statut des instances et du Replica Set"
    echo "  init      Initialiser le Replica Set (après start)"
    echo "  restart   Redémarrer toutes les instances"
    echo "  clean     Supprimer toutes les données (⚠️  destructif)"
    echo "  help      Afficher cette aide"
    echo ""
    echo "Exemple de premier démarrage:"
    echo "  1. $0 start    # Démarrer les instances"
    echo "  2. $0 init     # Initialiser le Replica Set"
    echo "  3. $0 status   # Vérifier le statut"
    echo ""
    echo "Configuration:"
    echo "  Replica Set : $REPLICA_SET_NAME"
    echo "  Ports       : ${PORTS[*]}"
    echo "  Données     : $BASE_DATA_DIR/{${DB_DIRS[*]}}"
    echo ""
}

#-------------------------------------------------------------------------------
# Point d'entrée principal
#-------------------------------------------------------------------------------

main() {
    case "${1:-help}" in
        start)
            create_directories
            start_instances
            ;;
        stop)
            stop_instances
            ;;
        status)
            check_status
            ;;
        init)
            init_replica_set
            ;;
        restart)
            stop_instances
            sleep 2
            create_directories
            start_instances
            ;;
        clean)
            clean_all
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            print_error "Commande inconnue : $1"
            show_help
            exit 1
            ;;
    esac
}

# Exécuter le script
main "$@"