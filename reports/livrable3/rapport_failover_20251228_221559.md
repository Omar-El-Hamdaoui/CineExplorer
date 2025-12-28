# Rapport de Tests de Tolérance aux Pannes
## Phase 3 - T3.2: Replica Set MongoDB

**Date:** 28/12/2025 22:15  
**Replica Set:** rs0  
**Nœuds:** db-1 (localhost:27017), db-2 (localhost:27018), db-3 (localhost:27019)

---

## Résumé

| Métrique | Valeur |
|----------|--------|
| Tests réussis | 7/7 |
| Taux de réussite | 100% |

---

## Détails des Tests

### Test 1: État initial

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:14:20.466103  

**Détails:**
```json
{
  "replica_set": "rs0",
  "members": [
    {
      "name": "localhost:27017",
      "state": "PRIMARY",
      "health": 1.0
    },
    {
      "name": "localhost:27018",
      "state": "SECONDARY",
      "health": 1.0
    },
    {
      "name": "localhost:27019",
      "state": "SECONDARY",
      "health": 1.0
    }
  ],
  "primary_count": 1,
  "secondary_count": 2
}
```

---

### Test 2: Écriture

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:14:23.497478  

**Détails:**
```json
{
  "documents_inserted": 3,
  "replication": {
    "27017": {
      "count": 3,
      "status": "OK"
    },
    "27018": {
      "count": 3,
      "status": "OK"
    },
    "27019": {
      "count": 3,
      "status": "OK"
    }
  }
}
```

---

### Test 3: Panne Primary

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:14:33.368324  

**Détails:**
```json
{
  "old_primary": "localhost:27017",
  "old_primary_node": "db-1",
  "new_primary": "localhost:27018",
  "new_primary_node": "db-2",
  "election_time_seconds": 4.0
}
```

---

### Test 4: Nouveau Primary

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:14:33.905966  

**Détails:**
```json
{
  "connected_port": 27018,
  "test_documents": 3,
  "movies": 291234,
  "movies_complete": 291234
}
```

---

### Test 5: Lecture

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:14:33.983035  

**Détails:**
```json
{
  "simple_read": "OK",
  "filtered_read": "OK",
  "aggregation": "OK"
}
```

---

### Test 6: Reconnexion

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:14:41.435601  

**Détails:**
```json
{
  "node": "db-1",
  "port": 27017,
  "new_state": "SECONDARY",
  "resync_time_seconds": 4.47
}
```

---

### Test 7: Double panne

**Statut:** ✅ PASS  
**Timestamp:** 2025-12-28T22:15:44.654076  

**Détails:**
```json
{
  "stopped_nodes": [
    "db-2",
    "db-1"
  ],
  "remaining_node": "db-3",
  "remaining_node_state": "SECONDARY",
  "write_operation": "BLOCKED (expected)",
  "conclusion": "Sans majorité (2/3 nœuds down), pas d'élection possible"
}
```

---

## Conclusion

Les tests démontrent le comportement du Replica Set MongoDB:

1. **Haute disponibilité**: En cas de panne du PRIMARY, un nouveau PRIMARY est élu automatiquement
2. **Réplication**: Les données sont répliquées sur tous les nœuds
3. **Récupération**: Un nœud peut rejoindre le cluster et se resynchroniser
4. **Majorité requise**: Sans majorité (2/3 nœuds down), les écritures sont bloquées

- **Temps d'élection**: 4.0 secondes
- **Temps de resync**: 4.47 secondes
