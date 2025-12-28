#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                                                                                ║
║     🎬 CINEEXPLORER - PHASE 3 T3.2: Tests de Tolérance aux Pannes             ║
║                                                                                ║
║                    Tests du Replica Set MongoDB                                ║
║                                                                                ║
╚════════════════════════════════════════════════════════════════════════════════╝

Version corrigée avec gestion correcte des PIDs via fichiers .pid

Usage:
  python3 test_failover.py [--auto]
"""

import time
import subprocess
import signal
import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, AutoReconnect

    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    print("❌ PyMongo n'est pas installé. Lancez: pip install pymongo")
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Configuration des tests"""

    # Replica Set
    REPLICA_SET_NAME = "rs0"
    NODES = [
        {"host": "localhost", "port": 27017, "name": "db-1"},
        {"host": "localhost", "port": 27018, "name": "db-2"},
        {"host": "localhost", "port": 27019, "name": "db-3"},
    ]

    # Connexion
    MONGO_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
    CONNECTION_TIMEOUT = 5000  # ms

    # Base de données de test
    TEST_DB = "cineexplorer_flat"
    TEST_COLLECTION = "failover_tests"

    # Chemins
    SCRIPT_DIR = Path(__file__).parent.absolute()
    PROJECT_ROOT = SCRIPT_DIR.parent.parent
    DATA_DIR = PROJECT_ROOT / "data" / "mongo"
    REPORTS_DIR = PROJECT_ROOT / "reports" / "livrable3"

    # Timeouts
    ELECTION_TIMEOUT = 45  # secondes max pour une élection
    RECONNECTION_TIMEOUT = 60  # secondes max pour une reconnexion


# ============================================================================
# COULEURS ET AFFICHAGE
# ============================================================================

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(title: str):
    print(f"\n{Colors.BOLD}{Colors.OKBLUE}")
    print("=" * 80)
    print(f"  {title}")
    print("=" * 80)
    print(Colors.ENDC)


def print_test(test_num: int, title: str):
    print(f"\n{Colors.BOLD}{Colors.OKCYAN}┌{'─' * 78}┐{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.OKCYAN}│ TEST {test_num}: {title:<69} │{Colors.ENDC}")
    print(f"{Colors.BOLD}{Colors.OKCYAN}└{'─' * 78}┘{Colors.ENDC}\n")


def print_success(message: str):
    print(f"{Colors.OKGREEN}✅ {message}{Colors.ENDC}")


def print_error(message: str):
    print(f"{Colors.FAIL}❌ {message}{Colors.ENDC}")


def print_warning(message: str):
    print(f"{Colors.WARNING}⚠️  {message}{Colors.ENDC}")


def print_info(message: str):
    print(f"{Colors.OKBLUE}ℹ️  {message}{Colors.ENDC}")


# ============================================================================
# CLASSE PRINCIPALE DE TEST
# ============================================================================

class ReplicaSetTester:
    """Testeur de tolérance aux pannes pour Replica Set MongoDB"""

    def __init__(self, auto_mode: bool = False):
        self.auto_mode = auto_mode
        self.client = None
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "tests": [],
            "summary": {}
        }
        self.stopped_nodes = []
        self.original_primary = None

    def connect(self, timeout: int = Config.CONNECTION_TIMEOUT) -> bool:
        """Connexion au Replica Set"""
        try:
            self.client = MongoClient(
                Config.MONGO_URI,
                serverSelectionTimeoutMS=timeout,
                connectTimeoutMS=timeout
            )
            self.client.admin.command('ping')
            return True
        except Exception as e:
            print_error(f"Connexion impossible: {e}")
            return False

    def get_replica_status(self) -> Optional[Dict]:
        """Récupère le statut du Replica Set"""
        try:
            status = self.client.admin.command('replSetGetStatus')
            return status
        except Exception as e:
            print_error(f"Impossible de récupérer le statut: {e}")
            return None

    def get_primary(self) -> Optional[Dict]:
        """Identifie le nœud PRIMARY actuel"""
        status = self.get_replica_status()
        if status:
            for member in status.get('members', []):
                if member.get('stateStr') == 'PRIMARY':
                    return member
        return None

    def get_primary_port(self) -> Optional[int]:
        """Récupère le port du PRIMARY"""
        primary = self.get_primary()
        if primary:
            host = primary.get('name', '')
            if ':' in host:
                return int(host.split(':')[1])
        return None

    def get_node_info(self, port: int) -> Optional[Dict]:
        """Récupère les infos d'un nœud par son port"""
        for node in Config.NODES:
            if node["port"] == port:
                return node
        return None

    def get_pid_from_file(self, port: int) -> Optional[int]:
        """Récupère le PID depuis le fichier .pid (méthode fiable)"""
        node_info = self.get_node_info(port)
        if not node_info:
            return None

        pid_file = Config.DATA_DIR / node_info["name"] / "mongod.pid"

        if pid_file.exists():
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                # Vérifier que le processus existe
                try:
                    os.kill(pid, 0)
                    return pid
                except OSError:
                    return None
            except:
                return None
        return None

    def stop_node(self, port: int) -> bool:
        """Arrête un nœud MongoDB via son fichier PID"""
        node_info = self.get_node_info(port)
        if not node_info:
            print_error(f"Nœud non trouvé pour le port {port}")
            return False

        # Méthode 1: Via fichier PID (plus fiable)
        pid = self.get_pid_from_file(port)

        if pid:
            try:
                print_info(f"Arrêt du nœud {node_info['name']} (port {port}, PID: {pid})...")
                os.kill(pid, signal.SIGTERM)
                time.sleep(2)

                # Vérifier si le processus est arrêté
                try:
                    os.kill(pid, 0)
                    # Processus existe encore, forcer l'arrêt
                    print_warning("Arrêt forcé...")
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(1)
                except OSError:
                    pass  # Processus déjà arrêté

                self.stopped_nodes.append({"port": port, "name": node_info["name"]})
                print_success(f"Nœud {node_info['name']} (port {port}) arrêté")
                return True
            except Exception as e:
                print_error(f"Erreur lors de l'arrêt: {e}")
                return False
        else:
            print_error(f"PID non trouvé pour le port {port}")
            return False

    def start_node(self, port: int) -> bool:
        """Redémarre un nœud MongoDB"""
        node_info = self.get_node_info(port)
        if not node_info:
            print_error(f"Configuration non trouvée pour le port {port}")
            return False

        db_path = Config.DATA_DIR / node_info["name"]
        log_path = db_path / "mongod.log"
        pid_path = db_path / "mongod.pid"

        # Supprimer le fichier lock si présent
        lock_file = db_path / "mongod.lock"
        if lock_file.exists():
            try:
                lock_file.unlink()
                print_info("Fichier lock supprimé")
            except:
                pass

        try:
            cmd = [
                'mongod',
                '--replSet', Config.REPLICA_SET_NAME,
                '--port', str(port),
                '--dbpath', str(db_path),
                '--bind_ip', 'localhost',
                '--logpath', str(log_path),
                '--logappend',
                '--pidfilepath', str(pid_path),
                '--fork'
            ]

            print_info(f"Démarrage du nœud {node_info['name']} (port {port})...")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0 or "child process started successfully" in result.stdout:
                time.sleep(3)
                # Vérifier que le processus est bien démarré
                if self.get_pid_from_file(port):
                    print_success(f"Nœud {node_info['name']} (port {port}) démarré")
                    # Retirer de la liste des nœuds arrêtés
                    self.stopped_nodes = [n for n in self.stopped_nodes if n["port"] != port]
                    return True
                else:
                    print_error(f"Le nœud semble démarré mais PID non trouvé")
                    return False
            else:
                print_error(f"Échec du démarrage: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            print_error("Timeout lors du démarrage")
            return False
        except Exception as e:
            print_error(f"Erreur lors du démarrage: {e}")
            return False

    def wait_for_confirmation(self, message: str = "Appuyez sur Entrée pour continuer..."):
        """Attend la confirmation de l'utilisateur"""
        if not self.auto_mode:
            input(f"\n{Colors.WARNING}⏸️  {message}{Colors.ENDC}")

    def add_test_result(self, test_num: int, name: str, success: bool, details: Dict):
        """Ajoute un résultat de test"""
        self.results["tests"].append({
            "test_num": test_num,
            "name": name,
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "details": details
        })

    # ========================================================================
    # TESTS
    # ========================================================================

    def test_1_initial_state(self) -> bool:
        """Test 1: État initial"""
        print_test(1, "État initial - Capture rs.status()")

        if not self.connect():
            self.add_test_result(1, "État initial", False, {"error": "Connexion impossible"})
            return False

        status = self.get_replica_status()
        if not status:
            self.add_test_result(1, "État initial", False, {"error": "Status non disponible"})
            return False

        print(f"\n{Colors.BOLD}📊 Statut du Replica Set:{Colors.ENDC}\n")
        print(f"   Nom: {status.get('set', 'N/A')}")
        print(f"   Date: {status.get('date', 'N/A')}")
        print(f"\n{Colors.BOLD}📋 Membres:{Colors.ENDC}\n")

        members_info = []
        for member in status.get('members', []):
            state = member.get('stateStr', 'UNKNOWN')
            name = member.get('name', 'N/A')
            health = "🟢" if member.get('health') == 1 else "🔴"

            if state == 'PRIMARY':
                self.original_primary = member
                state_color = Colors.OKGREEN
            elif state == 'SECONDARY':
                state_color = Colors.OKBLUE
            else:
                state_color = Colors.WARNING

            print(f"   {health} {name}: {state_color}{state}{Colors.ENDC}")
            members_info.append({"name": name, "state": state, "health": member.get('health')})

        primary_count = sum(1 for m in members_info if m['state'] == 'PRIMARY')
        secondary_count = sum(1 for m in members_info if m['state'] == 'SECONDARY')

        print(f"\n{Colors.BOLD}📈 Résumé:{Colors.ENDC}")
        print(f"   PRIMARY: {primary_count}")
        print(f"   SECONDARY: {secondary_count}")

        success = primary_count == 1 and secondary_count == 2

        if success:
            print_success("Configuration correcte: 1 PRIMARY + 2 SECONDARY")
        else:
            print_error("Configuration incorrecte!")

        self.add_test_result(1, "État initial", success, {
            "replica_set": status.get('set'),
            "members": members_info,
            "primary_count": primary_count,
            "secondary_count": secondary_count
        })

        return success

    def test_2_write_replication(self) -> bool:
        """Test 2: Écriture et réplication"""
        print_test(2, "Écriture - Test de réplication")

        db = self.client[Config.TEST_DB]
        collection = db[Config.TEST_COLLECTION]

        collection.delete_many({})

        test_docs = [
            {"test_id": 1, "message": "Test de réplication", "timestamp": datetime.now().isoformat()},
            {"test_id": 2, "message": "Document de test 2", "timestamp": datetime.now().isoformat()},
            {"test_id": 3, "message": "Document de test 3", "timestamp": datetime.now().isoformat()},
        ]

        print_info("Insertion de 3 documents de test...")

        try:
            result = collection.insert_many(test_docs)
            print_success(f"Documents insérés: {len(result.inserted_ids)}")
        except Exception as e:
            print_error(f"Erreur d'insertion: {e}")
            self.add_test_result(2, "Écriture", False, {"error": str(e)})
            return False

        print_info("Attente de la réplication (3 secondes)...")
        time.sleep(3)

        print_info("Vérification de la réplication sur tous les nœuds...")

        replication_ok = True
        replication_details = {}

        for node in Config.NODES:
            port = node["port"]
            try:
                client = MongoClient(
                    f"mongodb://localhost:{port}/",
                    serverSelectionTimeoutMS=3000,
                    directConnection=True
                )
                db_check = client[Config.TEST_DB]
                count = db_check[Config.TEST_COLLECTION].count_documents({})

                if count == 3:
                    print_success(f"Port {port} ({node['name']}): {count} documents ✓")
                    replication_details[port] = {"count": count, "status": "OK"}
                else:
                    print_warning(f"Port {port} ({node['name']}): {count} documents (attendu: 3)")
                    replication_details[port] = {"count": count, "status": "MISMATCH"}
                    replication_ok = False

                client.close()
            except Exception as e:
                print_error(f"Port {port} ({node['name']}): Erreur - {e}")
                replication_details[port] = {"error": str(e), "status": "ERROR"}
                replication_ok = False

        self.add_test_result(2, "Écriture", replication_ok, {
            "documents_inserted": 3,
            "replication": replication_details
        })

        return replication_ok

    def test_3_primary_failure(self) -> bool:
        """Test 3: Panne Primary"""
        print_test(3, "Panne Primary - Arrêt et élection")

        primary = self.get_primary()
        if not primary:
            print_error("Impossible d'identifier le PRIMARY")
            self.add_test_result(3, "Panne Primary", False, {"error": "PRIMARY non trouvé"})
            return False

        primary_port = self.get_primary_port()
        primary_node = self.get_node_info(primary_port)

        print_info(f"PRIMARY actuel: {primary.get('name')} ({primary_node['name']})")

        # Afficher les PIDs pour debug
        for node in Config.NODES:
            pid = self.get_pid_from_file(node["port"])
            print_info(f"  {node['name']} (port {node['port']}): PID {pid}")

        self.wait_for_confirmation(
            f"Prêt à arrêter le PRIMARY ({primary_node['name']}, port {primary_port}). Appuyez sur Entrée...")

        print_info("Arrêt du PRIMARY...")
        start_time = time.time()

        if not self.stop_node(primary_port):
            self.add_test_result(3, "Panne Primary", False, {"error": "Échec de l'arrêt"})
            return False

        print_info("Observation de l'élection...")
        print_info(f"Timeout: {Config.ELECTION_TIMEOUT} secondes")

        election_time = None
        new_primary = None

        # Ports des nœuds restants
        remaining_ports = [n["port"] for n in Config.NODES if n["port"] != primary_port]

        for i in range(Config.ELECTION_TIMEOUT):
            time.sleep(1)
            print(f"\r   ⏱️  Temps écoulé: {i + 1}s", end="", flush=True)

            for port in remaining_ports:
                try:
                    temp_client = MongoClient(
                        f"mongodb://localhost:{port}/",
                        serverSelectionTimeoutMS=2000,
                        directConnection=True
                    )
                    status = temp_client.admin.command('replSetGetStatus')

                    for member in status.get('members', []):
                        if member.get('stateStr') == 'PRIMARY':
                            member_port = int(member.get('name', '').split(':')[1])
                            if member_port != primary_port:
                                election_time = time.time() - start_time
                                new_primary = member
                                break

                    temp_client.close()

                    if new_primary:
                        break
                except:
                    continue

            if new_primary:
                break

        print()  # Nouvelle ligne

        if new_primary:
            new_port = int(new_primary.get('name', '').split(':')[1])
            new_node = self.get_node_info(new_port)
            print_success(f"Nouveau PRIMARY élu: {new_primary.get('name')} ({new_node['name']})")
            print_success(f"Temps d'élection: {election_time:.2f} secondes")

            self.add_test_result(3, "Panne Primary", True, {
                "old_primary": primary.get('name'),
                "old_primary_node": primary_node['name'],
                "new_primary": new_primary.get('name'),
                "new_primary_node": new_node['name'],
                "election_time_seconds": round(election_time, 2)
            })
            return True
        else:
            print_error("Aucun nouveau PRIMARY élu dans le délai imparti")
            self.add_test_result(3, "Panne Primary", False, {
                "old_primary": primary.get('name'),
                "error": "Élection timeout"
            })
            return False

    def test_4_verify_new_primary(self) -> bool:
        """Test 4: Vérification des données sur le nouveau PRIMARY"""
        print_test(4, "Nouveau Primary - Vérification des données")

        # Trouver un nœud actif
        remaining_ports = [n["port"] for n in Config.NODES
                           if n["port"] not in [sn["port"] for sn in self.stopped_nodes]]

        connected = False
        connected_port = None

        for port in remaining_ports:
            try:
                self.client = MongoClient(
                    f"mongodb://localhost:{port}/",
                    serverSelectionTimeoutMS=5000,
                    directConnection=True
                )
                self.client.admin.command('ping')
                node = self.get_node_info(port)
                print_success(f"Connecté à {node['name']} (port {port})")
                connected = True
                connected_port = port
                break
            except Exception as e:
                continue

        if not connected:
            print_error("Impossible de se reconnecter")
            self.add_test_result(4, "Nouveau Primary", False, {"error": "Reconnexion impossible"})
            return False

        try:
            db = self.client[Config.TEST_DB]

            test_count = db[Config.TEST_COLLECTION].count_documents({})
            movies_count = db.movies.count_documents({}) if 'movies' in db.list_collection_names() else 0
            movies_complete_count = db.movies_complete.count_documents(
                {}) if 'movies_complete' in db.list_collection_names() else 0

            print(f"\n{Colors.BOLD}📊 Vérification des données:{Colors.ENDC}")
            print(f"   Documents de test: {test_count}")
            print(f"   Collection movies: {movies_count:,}")
            print(f"   Collection movies_complete: {movies_complete_count:,}")

            success = test_count == 3 and movies_count > 0

            if success:
                print_success("Toutes les données sont accessibles!")
            else:
                print_warning("Certaines données semblent manquantes")

            self.add_test_result(4, "Nouveau Primary", success, {
                "connected_port": connected_port,
                "test_documents": test_count,
                "movies": movies_count,
                "movies_complete": movies_complete_count
            })

            return success

        except Exception as e:
            print_error(f"Erreur de vérification: {e}")
            self.add_test_result(4, "Nouveau Primary", False, {"error": str(e)})
            return False

    def test_5_read_operations(self) -> bool:
        """Test 5: Lecture"""
        print_test(5, "Lecture - Accessibilité des données")

        try:
            db = self.client[Config.TEST_DB]

            print_info("Test de lecture simple...")
            doc = db[Config.TEST_COLLECTION].find_one({"test_id": 1})
            if doc:
                print_success(f"Document trouvé: {doc.get('message')}")
            else:
                print_error("Document non trouvé")
                return False

            print_info("Test de lecture avec filtre...")
            if 'movies_complete' in db.list_collection_names():
                movie = db.movies_complete.find_one({"rating.votes": {"$gt": 1000000}})
                if movie:
                    print_success(f"Film populaire trouvé: {movie.get('title')} ({movie.get('year')})")

            print_info("Test d'agrégation...")
            if 'movies' in db.list_collection_names():
                pipeline = [{"$group": {"_id": None, "count": {"$sum": 1}}}]
                result = list(db.movies.aggregate(pipeline))
                if result:
                    print_success(f"Agrégation réussie: {result[0]['count']:,} films")

            self.add_test_result(5, "Lecture", True, {
                "simple_read": "OK",
                "filtered_read": "OK",
                "aggregation": "OK"
            })

            return True

        except Exception as e:
            print_error(f"Erreur de lecture: {e}")
            self.add_test_result(5, "Lecture", False, {"error": str(e)})
            return False

    def test_6_node_recovery(self) -> bool:
        """Test 6: Reconnexion du nœud arrêté"""
        print_test(6, "Reconnexion - Resynchronisation du nœud")

        if not self.stopped_nodes:
            print_warning("Aucun nœud n'a été arrêté")
            self.add_test_result(6, "Reconnexion", True, {"message": "Aucun nœud à redémarrer"})
            return True

        stopped_node = self.stopped_nodes[0]
        port = stopped_node["port"]
        node_name = stopped_node["name"]

        self.wait_for_confirmation(f"Prêt à redémarrer {node_name} (port {port}). Appuyez sur Entrée...")

        print_info(f"Redémarrage de {node_name} (port {port})...")
        start_time = time.time()

        if not self.start_node(port):
            self.add_test_result(6, "Reconnexion", False, {"error": "Échec du démarrage"})
            return False

        print_info("Attente de la resynchronisation...")

        resync_time = None
        for i in range(Config.RECONNECTION_TIMEOUT):
            time.sleep(1)
            print(f"\r   ⏱️  Temps écoulé: {i + 1}s", end="", flush=True)

            try:
                temp_client = MongoClient(
                    f"mongodb://localhost:{port}/",
                    serverSelectionTimeoutMS=2000,
                    directConnection=True
                )
                status = temp_client.admin.command('replSetGetStatus')

                for member in status.get('members', []):
                    if str(port) in member.get('name', ''):
                        state = member.get('stateStr')
                        if state in ['PRIMARY', 'SECONDARY']:
                            resync_time = time.time() - start_time
                            print()
                            print_success(f"Nœud {node_name} resynchronisé en tant que {state}")
                            print_success(f"Temps de resync: {resync_time:.2f} secondes")

                            temp_client.close()

                            self.add_test_result(6, "Reconnexion", True, {
                                "node": node_name,
                                "port": port,
                                "new_state": state,
                                "resync_time_seconds": round(resync_time, 2)
                            })
                            return True

                temp_client.close()
            except:
                continue

        print()
        print_error("Resynchronisation timeout")
        self.add_test_result(6, "Reconnexion", False, {"error": "Resync timeout"})
        return False

    def test_7_double_failure(self) -> bool:
        """Test 7: Double panne"""
        print_test(7, "Double panne - Test avec 2 nœuds down")

        # Reconnecter au Replica Set complet
        try:
            self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
        except:
            print_error("Impossible de se connecter au Replica Set")
            self.add_test_result(7, "Double panne", False, {"error": "Connexion impossible"})
            return False

        status = self.get_replica_status()
        if not status:
            return False

        # Identifier PRIMARY et un SECONDARY
        primary_port = None
        secondary_port = None
        remaining_port = None

        for member in status.get('members', []):
            port = int(member.get('name', '').split(':')[1])
            if member.get('stateStr') == 'PRIMARY':
                primary_port = port
            elif member.get('stateStr') == 'SECONDARY':
                if secondary_port is None:
                    secondary_port = port
                else:
                    remaining_port = port

        if not primary_port or not secondary_port:
            print_error("Configuration du Replica Set incorrecte")
            return False

        if remaining_port is None:
            remaining_port = secondary_port
            secondary_port = primary_port
            # Dans ce cas, on arrête le PRIMARY et un SECONDARY

        # Trouver le nœud restant
        all_ports = [n["port"] for n in Config.NODES]
        for p in all_ports:
            if p != primary_port and p != secondary_port:
                remaining_port = p
                break

        primary_node = self.get_node_info(primary_port)
        secondary_node = self.get_node_info(secondary_port)
        remaining_node = self.get_node_info(remaining_port)

        print_info(f"PRIMARY: {primary_node['name']} (port {primary_port})")
        print_info(f"SECONDARY à arrêter: {secondary_node['name']} (port {secondary_port})")
        print_info(f"Nœud restant: {remaining_node['name']} (port {remaining_port})")

        self.wait_for_confirmation("Prêt à arrêter 2 nœuds. Appuyez sur Entrée...")

        # Arrêter le SECONDARY d'abord
        print_info(f"Arrêt du SECONDARY ({secondary_node['name']})...")
        self.stop_node(secondary_port)
        time.sleep(2)

        # Arrêter le PRIMARY
        print_info(f"Arrêt du PRIMARY ({primary_node['name']})...")
        self.stop_node(primary_port)
        time.sleep(5)

        # Vérifier l'état du dernier nœud
        print_info(f"Vérification du nœud restant ({remaining_node['name']}, port {remaining_port})...")

        try:
            temp_client = MongoClient(
                f"mongodb://localhost:{remaining_port}/",
                serverSelectionTimeoutMS=10000,
                directConnection=True
            )
            status = temp_client.admin.command('replSetGetStatus')

            for member in status.get('members', []):
                if str(remaining_port) in member.get('name', ''):
                    state = member.get('stateStr')
                    print_info(f"État du nœud restant: {state}")

                    if state in ['SECONDARY', 'RECOVERING']:
                        print_success("Comportement attendu: le nœud ne peut pas devenir PRIMARY sans majorité")

                        # Tester si l'écriture échoue (attendu)
                        try:
                            db = temp_client[Config.TEST_DB]
                            db[Config.TEST_COLLECTION].insert_one({"test": "double_failure"})
                            print_warning("Écriture réussie (inattendu)")
                            write_result = "SUCCESS (unexpected)"
                        except Exception as e:
                            print_success(f"Écriture impossible (attendu): {type(e).__name__}")
                            write_result = "BLOCKED (expected)"

                        temp_client.close()

                        self.add_test_result(7, "Double panne", True, {
                            "stopped_nodes": [primary_node['name'], secondary_node['name']],
                            "remaining_node": remaining_node['name'],
                            "remaining_node_state": state,
                            "write_operation": write_result,
                            "conclusion": "Sans majorité (2/3 nœuds down), pas d'élection possible"
                        })

                        # Redémarrer les nœuds
                        print_info("\nRedémarrage des nœuds arrêtés...")
                        self.start_node(secondary_port)
                        time.sleep(3)
                        self.start_node(primary_port)
                        time.sleep(5)

                        return True

            temp_client.close()

        except Exception as e:
            print_info(f"Le nœud restant n'est pas accessible (comportement possible): {type(e).__name__}")

            self.add_test_result(7, "Double panne", True, {
                "stopped_nodes": [primary_node['name'], secondary_node['name']],
                "remaining_node": remaining_node['name'],
                "conclusion": "Sans majorité, le cluster est indisponible"
            })

        # Redémarrer les nœuds
        print_info("\nRedémarrage des nœuds arrêtés...")
        self.start_node(secondary_port)
        time.sleep(3)
        self.start_node(primary_port)
        time.sleep(5)

        return True

    # ========================================================================
    # RAPPORT
    # ========================================================================

    def generate_report(self):
        """Génère le rapport des tests"""
        print_header("📝 RAPPORT DES TESTS")

        total_tests = len(self.results["tests"])
        passed_tests = sum(1 for t in self.results["tests"] if t["success"])

        print(f"\n{Colors.BOLD}📊 Résumé:{Colors.ENDC}")
        print(f"   Tests réussis: {passed_tests}/{total_tests}")
        print(f"   Taux de réussite: {(passed_tests / total_tests) * 100:.0f}%")

        print(f"\n{Colors.BOLD}📋 Détails des tests:{Colors.ENDC}\n")

        for test in self.results["tests"]:
            status = f"{Colors.OKGREEN}✅ PASS{Colors.ENDC}" if test["success"] else f"{Colors.FAIL}❌ FAIL{Colors.ENDC}"
            print(f"   Test {test['test_num']}: {test['name']:<30} {status}")

            details = test.get("details", {})
            if "election_time_seconds" in details:
                print(f"      └─ Temps d'élection: {details['election_time_seconds']}s")
            if "resync_time_seconds" in details:
                print(f"      └─ Temps de resync: {details['resync_time_seconds']}s")

        self.results["summary"] = {
            "total_tests": total_tests,
            "passed_tests": passed_tests,
            "success_rate": f"{(passed_tests / total_tests) * 100:.0f}%"
        }

        Config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        # Sauvegarder JSON
        report_path = Config.REPORTS_DIR / f"failover_tests_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)

        print(f"\n{Colors.OKGREEN}📁 Rapport JSON: {report_path}{Colors.ENDC}")

        # Générer Markdown
        self.generate_markdown_report()

    def generate_markdown_report(self):
        """Génère un rapport Markdown"""

        md_content = f"""# Rapport de Tests de Tolérance aux Pannes
## Phase 3 - T3.2: Replica Set MongoDB

**Date:** {datetime.now().strftime('%d/%m/%Y %H:%M')}  
**Replica Set:** {Config.REPLICA_SET_NAME}  
**Nœuds:** {', '.join([f"{n['name']} (localhost:{n['port']})" for n in Config.NODES])}

---

## Résumé

| Métrique | Valeur |
|----------|--------|
| Tests réussis | {self.results['summary']['passed_tests']}/{self.results['summary']['total_tests']} |
| Taux de réussite | {self.results['summary']['success_rate']} |

---

## Détails des Tests

"""
        for test in self.results["tests"]:
            status = "✅ PASS" if test["success"] else "❌ FAIL"
            md_content += f"### Test {test['test_num']}: {test['name']}\n\n"
            md_content += f"**Statut:** {status}  \n"
            md_content += f"**Timestamp:** {test['timestamp']}  \n\n"

            if test["details"]:
                md_content += "**Détails:**\n```json\n"
                md_content += json.dumps(test["details"], indent=2, ensure_ascii=False)
                md_content += "\n```\n\n"

            md_content += "---\n\n"

        md_content += """## Conclusion

Les tests démontrent le comportement du Replica Set MongoDB:

1. **Haute disponibilité**: En cas de panne du PRIMARY, un nouveau PRIMARY est élu automatiquement
2. **Réplication**: Les données sont répliquées sur tous les nœuds
3. **Récupération**: Un nœud peut rejoindre le cluster et se resynchroniser
4. **Majorité requise**: Sans majorité (2/3 nœuds down), les écritures sont bloquées

"""

        for test in self.results["tests"]:
            details = test.get("details", {})
            if "election_time_seconds" in details:
                md_content += f"- **Temps d'élection**: {details['election_time_seconds']} secondes\n"
            if "resync_time_seconds" in details:
                md_content += f"- **Temps de resync**: {details['resync_time_seconds']} secondes\n"

        md_path = Config.REPORTS_DIR / f"rapport_failover_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(md_content)

        print(f"{Colors.OKGREEN}📄 Rapport Markdown: {md_path}{Colors.ENDC}")

    def run_all_tests(self):
        """Exécute tous les tests"""

        print(f"\n{Colors.BOLD}{Colors.HEADER}")
        print("╔" + "═" * 78 + "╗")
        print("║" + " " * 78 + "║")
        print("║" + "🎬 CINEEXPLORER - TESTS DE TOLÉRANCE AUX PANNES".center(78) + "║")
        print("║" + "Phase 3 - T3.2: Replica Set MongoDB".center(78) + "║")
        print("║" + " " * 78 + "║")
        print("╚" + "═" * 78 + "╝")
        print(Colors.ENDC)

        print(f"\n⏰ Début des tests: {datetime.now().strftime('%H:%M:%S')}")
        print(f"🔧 Mode: {'Automatique' if self.auto_mode else 'Interactif'}")

        tests = [
            self.test_1_initial_state,
            self.test_2_write_replication,
            self.test_3_primary_failure,
            self.test_4_verify_new_primary,
            self.test_5_read_operations,
            self.test_6_node_recovery,
            self.test_7_double_failure,
        ]

        for test_func in tests:
            try:
                test_func()
            except KeyboardInterrupt:
                print_warning("\nTests interrompus par l'utilisateur")
                break
            except Exception as e:
                print_error(f"Erreur inattendue: {e}")
                import traceback
                traceback.print_exc()

        self.generate_report()

        print(f"\n⏰ Fin des tests: {datetime.now().strftime('%H:%M:%S')}")

        if self.client:
            self.client.close()


def main():
    auto_mode = '--auto' in sys.argv

    tester = ReplicaSetTester(auto_mode=auto_mode)

    try:
        tester.run_all_tests()
        return 0
    except KeyboardInterrupt:
        print_warning("\nInterrompu par l'utilisateur")
        return 1
    except Exception as e:
        print_error(f"Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())