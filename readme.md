# SolidEngine

Moteur de stockage, d'ordonnancement et de distribution de jobs en Rust : shards sans partage, livraison at-least-once, leases et fencing, WAL et snapshots, protocole binaire (Unix socket / TCP).

```
cargo build --release
./target/release/solid-engine-server --config solid-engine.toml
cargo test --workspace
```

Documentation d'intégration SolidJobs : [docs/MIGRATION.md](docs/MIGRATION.md).

## Durabilité

Chaque shard possède son WAL ; en `balanced` et `strict`, un thread dédié par shard écrit et synchronise (le shard n'attend jamais le disque).

| Mode | Réponse envoyée | Perte possible après crash |
|---|---|---|
| `memory` | après la mutation en mémoire | tout |
| `balanced` | après la mutation, **avant** le fsync (fsync toutes les `sync_interval_ms`) | les opérations confirmées depuis le dernier fsync |
| `strict` | après le fsync du lot contenant l'opération (group commit) | aucune opération confirmée |

Une erreur d'I/O WAL met le shard en échec définitif : plus aucun succès n'est confirmé (tests : `tests/fault.rs`). En `balanced`, l'erreur est détectée de façon asynchrone, donc quelques réponses peuvent sortir avant sa détection.

## Authentification TCP (phase 6)

Si un jeton est configuré, toute connexion **TCP** doit envoyer `AUTH` (message 11) avant n'importe quelle commande. Une commande préalable ou un mauvais jeton renvoie `Unauthorized` (code 9) puis ferme la connexion. Un `AUTH` répété sur une connexion déjà authentifiée est accepté.

Source du jeton, par priorité : variable `SOLID_ENGINE_AUTH_TOKEN`, puis `server.auth_token_file` (première ligne), puis `server.auth_token`. Aucun jeton par défaut ; un jeton vide est refusé au démarrage. Sans jeton, le comportement est inchangé. Valable en modes classique, réacteur et Zero-Dispatch (tests `tests/auth.rs`, 16 suites vertes dans les trois modes).

Le socket Unix n'est pas soumis au jeton : on le protège par ses permissions (`server.unix_socket_mode`, p. ex. `0o600`).

Limites connues :
- la comparaison est en temps constant, mais la longueur du jeton peut fuiter ;
- pas de limitation de tentatives par IP, pas de délai dédié pour les connexions non authentifiées (fermeture au premier échec ou à la première commande non-AUTH) ;
- le jeton transite en clair : **ce n'est pas un substitut à TLS** ; sur un réseau non fiable, utiliser un proxy TLS ;
- le coût CPU/job de l'authentification n'a pas encore été mesuré (une vérification par connexion, pas par commande, à confirmer par benchmark).

## Benchmark (phase 5)

Banc : `../benchmark_solid_engine` (crate indépendante).

```
cargo build --release            # dans solid-engine/
cd ../benchmark_solid_engine && cargo build --release
./target/release/benchmark_solid_engine --jobs 30000 --repeats 3 --workers 1,8,32 \
    --targets redis,engine-memory,engine-balanced
./target/release/benchmark_solid_engine --jobs 6000 --repeats 3 --workers 1,8,32,128 --targets engine-strict
```

**Conditions** : macOS 27.0.1 arm64, 12 CPU, rustc 1.97.1, Redis 8.10.0, TCP loopback, serveur en processus séparé.
Workload noop : chaque worker boucle `push → claim → ack` sur une même queue, payload 256 B, médiane de 3 répétitions (30 000 jobs par run, 6 000 pour `strict`).
Redis reproduit le schéma de SolidJobs (`LPUSH`, `RPOPLPUSH`, `LREM`), **sans persistance** : comparable à `engine-memory` seulement. La comparaison durable (Redis AOF) n'est pas encore faite. Latences = cycle complet (3 requêtes).

Jobs/s (rapport à Redis entre parenthèses) :

| workers | redis | engine-memory | engine-balanced | engine-strict |
|---:|---:|---:|---:|---:|
| 1 | 16 392 | 10 675 (0,65×) | 13 609 (0,83×) | 137 (0,01×) |
| 8 | 42 756 | 30 822 (0,72×) | 30 444 (0,71×) | 336 (0,01×) |
| 32 | 52 503 | 34 139 (0,65×) | 33 222 (0,63×) | 981 (0,02×) |
| 128 | – | – | – | 3 416 |

Latences (µs) :

| cible | workers | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| redis | 1 | 58 | 106 | 152 |
| engine-memory | 1 | 91 | 163 | 268 |
| engine-balanced | 1 | 68 | 158 | 286 |
| engine-strict | 1 | 7 127 | 11 992 | 19 905 |
| redis | 32 | 585 | 904 | 973 |
| engine-memory | 32 | 911 | 1 082 | 18 201 |
| engine-balanced | 32 | 944 | 1 247 | 13 926 |
| engine-strict | 32 | 31 269 | 52 231 | 60 981 |

Group commit en `strict` (compteurs instrumentés temporairement, cumul des 4 shards) :

| workers | opérations par fsync | durée moyenne d'un fsync |
|---:|---:|---:|
| 32 | 5,5 | 5,0 ms |
| 128 | 21 | 5,2 ms |

La taille des lots suit la concurrence disponible par shard ; le plafond de `strict` est le coût du fsync (≈ 3 ms à vide, ≈ 5 ms sous charge sur macOS).

RSS : Redis ≈ 8 MB ; SolidEngine ≈ 28 MB (memory), ≈ 41 MB (balanced), ≈ 23 MB (strict).

### Ce qui a changé le plus

Avant le writer WAL dédié, `balanced` faisait 2 973 / 3 824 / 6 227 jobs/s (1 / 8 / 32 workers). La mesure a montré que le shard perdait son temps dans ses propres `write` (~30 µs chacun, pics de plusieurs ms) sur un fichier en cours de fsync, et non dans le fsync lui-même. Un simple thread de fsync séparé avait même aggravé les chiffres ; seul un thread possédant tout le WAL a réglé le problème.

### Pipelining (8 connexions, `--window N` jobs indépendants en vol par connexion)

Chaque slot avance seul (push → claim → ack → nouveau push), `request_id` = slot. Médiane de 3 runs, 30 000 jobs, 256 B.

| fenêtre | balanced jobs/s | balanced p99 | strict jobs/s | strict p99 |
|---:|---:|---:|---:|---:|
| 1 | 31 239 | 391 µs | 316 | 43 ms |
| 8 | 45 138 | 2,0 ms | 1 915 | 57 ms |
| 32 | 58 009 | 15,7 ms | 7 056 | 61 ms |

Strict sans pipelining : 996 jobs/s (32 connexions) et 3 567 jobs/s (128 connexions).

La latence est celle d'un job complet (3 opérations) dans une file de `fenêtre × connexions` requêtes : elle croît avec la fenêtre.
Ce tableau précède le client bufferisé et les optimisations de la couche connexion ci-dessous ; Redis pipeliné est comparé dans la section suivante.

### Ce qui a changé le plus

Avant le writer WAL dédié, `balanced` faisait 2 973 / 3 824 / 6 227 jobs/s (1 / 8 / 32 workers). La mesure a montré que le shard perdait son temps dans ses propres `write` (~30 µs chacun, pics de plusieurs ms) sur un fichier en cours de fsync, et non dans le fsync lui-même. Un simple thread de fsync séparé avait même aggravé les chiffres ; seul un thread possédant tout le WAL a réglé le problème.

Plafond de `strict` en pipelining : avec 256 requêtes en vol, le writer ne voyait que 4,4 opérations par fsync. La cause était dans le serveur : le claim passait par un appel bloquant, donc le thread de lecture d'une connexion attendait le fsync de chaque claim, shard par shard, et sérialisait la connexion. Avec `try_claim_async` (parcours des shards par callbacks), le writer regroupe environ 49 opérations par fsync (fsync moyen 5,1 ms au lieu de 4,4 ms) et `strict` passe de ≈ 940 à 7 056 jobs/s dans la même configuration.

### Comparaison Redis avec scripts Lua (la plus pertinente)

Même protocole (8 connexions, 30 000 jobs, 256 B, médiane de 3, macOS, défauts de SolidEngine : spin 0 µs). Trois scripts Lua atomiques : push dédoublonné par id, claim avec lease + fencing token + numéro de réservation, ack conditionnel (réservation et token). Toujours un sous-ensemble : ni délais, ni retries, ni expiration de lease. Cibles `redis-lua`, `redis-lua-everysec`, `redis-lua-always`. Durabilités non équivalentes (voir plus haut) ; aucun test de crash côté Redis.

| fenêtre | redis simplifié | redis-lua | redis-lua everysec | redis-lua always | SE memory | SE balanced | SE strict |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 42 483 | 28 503 | 22 246 | 462 | 31 523 | 32 302 | 306 |
| 8 | 201 306 | 59 108 | 42 406 | 3 715 | 45 153 | 48 454 | 1 792 |
| 32 | 410 897 | 69 737 | 49 632 | 10 358 | 47 812 | 58 062 | 7 319 |

p99 à la fenêtre 32 : redis-lua 5,6 ms, everysec 7,8 ms, always 32 ms ; SE memory 5,9 ms, balanced 5,6 ms, strict 57,5 ms.
CPU serveur par job (fenêtre 32, µs usr + sys) : redis simplifié ≈ 2, redis-lua ≈ 14, SolidEngine ≈ 90–115.

Lecture : le workload Redis simplifié surestimait l'écart (÷ 6 dès que Redis fait le même travail). Redis-lua reste devant SolidEngine memory (+46 % à la fenêtre 32) ; SolidEngine balanced dépasse redis-lua everysec (+17 %) et redis-lua always dépasse SolidEngine strict (+41 %, avec une p99 meilleure). Écarts inférieurs à 15 % non fiables (variabilité entre runs). Le coût CPU par job de SolidEngine (≈ 6× Redis-lua) est le principal point à réduire.

### Comparaison Redis simplifiée — référence historique (même fenêtre, TCP, 8 connexions, 30 000 jobs, 256 B, médiane de 3)

Mesure historique, **avant** le client bufferisé et les optimisations de la couche connexion. Redis 8.x, sans RDB. Le workload Redis est **simplifié** (`LPUSH`, `RPOPLPUSH`, `LREM`) : ni leases, ni fencing, ni dédoublonnage, ni délais, ni `max_attempts`. Les garanties ne sont pas équivalentes ; un Redis à base de scripts Lua serait la comparaison juste (non fait). Redis `always` utilise probablement `F_FULLFSYNC` sur macOS (non vérifié). Aucun test de crash côté Redis.

| fenêtre | redis | redis everysec | redis always | SE memory | SE balanced | SE strict |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 42 718 | 35 931 | 464 | 30 972 | 31 269 | 321 |
| 8 | 212 444 | 156 188 | 3 570 | 39 012 | 43 455 | 2 063 |
| 32 | 365 696 | 243 373 | 13 817 | 42 835 | 50 224 | 7 359 |

Redis pipeliné est nettement devant (≈ 8,5× SolidEngine memory à la fenêtre 32). Les chiffres balanced ci-dessus ne dépassent pas Redis : une comparaison antérieure avec Redis non pipeliné était erronée.

### Diagnostics de performance (macOS arm64, 12 CPU)

**Moteur seul, sans réseau** (`examples/engine_only.rs`, memory, un run, exploratoire — à ne pas comparer à Redis) :

| connexions × fenêtre | jobs/s |
|---|---:|
| 1×1 | 48 989 |
| 1×32 | 508 422 |
| 8×8 | 328 094 |
| 8×32 | 415 477 |

Le moteur n'est pas le goulot : le serveur TCP plafonne à ≈ 43–78 k jobs/s sur le même schéma, avec ≈ 80 % de temps CPU noyau (réveils de threads, appels système).

**Changements de la couche connexion** : lecteur bufferisé (64 Ko), écrivain qui regroupe toutes les réponses prêtes dans un `write`, et attente active (spin) avant de dormir, dans l'écrivain et dans les shards. Le client du banc est aussi bufferisé (l'ancien faisait 3 `recv` par trame, donc biaisé contre le moteur) : le gain de 47 k à 75 k jobs/s ne peut pas être attribué au seul spin.

**Spin : valeur par défaut 0 µs.** Variables avancées (lues une fois, plafonnées à 100 µs, valeur invalide → défaut avec avertissement) :
- `SOLID_ENGINE_SPIN_WRITER_US`, `SOLID_ENGINE_SPIN_SHARD_US` : budget de spin en µs (0 = désactivé).
- `SOLID_ENGINE_SPIN_BACKOFF` (0–6, défaut 2) : après un spin infructueux, saute jusqu'à 2^n − 1 spins suivants ; 0 = spin fixe.
- `SOLID_ENGINE_SPIN_STATS` : affiche sur stderr les compteurs `attempts`, `hits`, `skipped`.

Ablation (memory, 8×32, 240 000 jobs, spin fixe, médiane de 3 ; CPU en µs/job usr + sys) :

| writer \ shard | 0 µs | 10 µs | 30 µs |
|---|---|---|---|
| 0 | 50 291 (17 + 68) | 53 349 (30 + 61) | 57 330 (43 + 51) |
| 10 | 57 622 (29 + 56) | 64 785 (36 + 45) | 65 667 (50 + 41) |
| 30 | 67 154 (40 + 42) | 69 484 (49 + 39) | 77 020 (52 + 32) |

Le spin de l'écrivain pèse plus que celui des shards ; il échange du CPU noyau contre du CPU utilisateur. Les changements de contexte ne sont pas mesurables sous macOS (`ps` ne les expose pas) : le mécanisme (coût des réveils) est inféré, non isolé.

Campagne entrelacée (4 rounds, ordres alternés, serveur redémarré à chaque run, médianes ; faible charge = 1 worker, ≈ 690 jobs/s) :

| config | saturation jobs/s | p99 | CPU/job sat. | CPU/job faible charge | hit rate sat. / faible |
|---|---:|---:|---:|---:|---|
| spin 0 (défaut) | 51 941 | 6,2 ms | 80 | 136 | n/a |
| recul 3 | 62 965 | 5,6 ms | 95 | 211 | 69 % / ~7 % |
| recul 63 | 55 800 | 5,8 ms | 101 | 150 | 68 % / ~10 % |
| fixe 30 | 77 775 | 5,7 ms | 87 | 335 | 71 % / 8 % |

Le spin est utile sous charge (hit rate ≈ 70 %) et presque entièrement gaspillé à faible charge (6–10 %). Aucun réglage ne gagne partout. CPU **au repos** (20 s, 0 ou 8 connexions inactives) : 0,10–0,15 % d'un cœur pour toutes les configurations (résolution de `ps`) ; le spin ne coûte rien sans activité. Le retour au repos après une rafale n'est pas mesuré.

Réserves : variabilité de 10 à 60 % entre rounds, compteurs atomiques globaux non comparés à une version sans instrumentation, pas de Linux. Le RSS (memory ≈ 30 Mo, balanced ≈ 45 Mo, Redis ≈ 8 Mo) n'est pas expliqué et croît avec le nombre de jobs (jobs terminaux apparemment conservés).

Reproduction : `cargo build --release` dans `solid-engine` et `benchmark_solid_engine`, puis par exemple `SOLID_ENGINE_SPIN_WRITER_US=30 SOLID_ENGINE_SPIN_SHARD_US=30 benchmark_solid_engine --targets engine-memory --workers 8 --jobs 240000 --window 32 --repeats 3` ; `BENCH_THINK_US=1000 … --workers 1 --window 2` pour la faible charge.

### Lecture honnête

- **Débit** : sans persistance et sans pipelining, SolidEngine fait 0,63 à 0,83× Redis. L'objectif de la spec (+20 à +30 %) n'est **pas atteint** dans cette configuration. Avec la même fenêtre, Redis pipeliné reste devant (voir la comparaison ci-dessus).
- **Mémoire** : supérieure à Redis (objectif non atteint). La taille de la file du writer (65 536 messages) n'a pas été optimisée.
- **Latence p99 à faible charge** : 158 à 163 µs, sous l'objectif de 5 ms.
- **`strict`** : l'objectif de 5 000 jobs/s est atteint seulement avec du pipelining (7 056 jobs/s, 8 connexions, fenêtre 32). Sans pipelining il reste limité par le fsync (`sync_data` ≈ 3 à 5 ms sur macOS, via F_FULLFSYNC) : 981 jobs/s à 32 workers.
- **Attente de claim** : un claim vide avec `wait_ms > 0` crée un thread ; un scheduler d'attentes borné serait préférable sous forte concurrence.
- **Non mesuré** : Redis Lua avec délais, retries et expiration de lease, Linux, un seul workload (noop), pas de jobs différés, retries, gros payloads, multi-queues, changements de contexte, latence des claims vides. À 512 workers le banc subit des `Connection reset` dont la cause n'est pas identifiée.
- **Tests de multiplexage** (`tests/pipelining.rs`, 3 modes) : réponses hors ordre sur 256 requêtes en vol, et 24 déconnexions brutales avec limite de 4 connexions (libération des connexions). Pas de test de fuite mémoire, ni loom / ThreadSanitizer.
- **Frontière de durabilité** (`tests/durable_ack.rs`) : en `strict`, aucun succès tant que le fsync du shard concerné est retenu ; un fsync en échec ne confirme rien et laisse le shard en échec ; le fsync d'un shard ne libère pas les réponses d'un autre ; en `balanced` la réponse précède le fsync. Il n'y a pas de compteur `durable_seq` explicite : la garantie est structurelle (les réponses voyagent dans le `Commit` du shard et sont libérées par son writer après le fsync).
- **Limite connue du moteur** : le scan multi-shards d'un claim n'est pas atomique : sous forte contention, un claim peut renvoyer « vide » alors qu'un job vient d'être poussé (le banc réessaie).

Prochaines étapes : instrumenter le chemin réseau du réacteur, réduire les échanges entre threads, profiler le chemin mémoire (routage et mailboxes).

### Linux (Docker Desktop), topologie CPU, Redis Lua et réacteur expérimental

Environnement : conteneur `rust:1-bookworm` dans Docker Desktop (VM LinuxKit, 12 CPU partagés avec macOS), mode Memory, affinité de claim A2, générateur épinglé sur les CPU 0–5, serveur sur les CPU 6–7 (`taskset`). Médianes de 3 passes entrelacées. Le CPU est celui du processus serveur (`/proc`). Scripts : `benchmark_solid_engine/scripts/` (`pin_server.sh`, `pin_redis.sh`, `campaign_redis.sh`, `thread_cpu.py`, `perf_attach.sh`).

**Effet du nombre de CPU autorisés** (2×32, 80 000 jobs/s imposés) : 2 CPU ≈ 20,6 µs/job, 6 CPU ≈ 30, 12 CPU ≈ 33. L'identité des CPU (paires voisines ou éloignées) n'a aucun effet mesurable ; la topologie exposée par la VM est plate. Les migrations (≈ 0,03–0,15/job) et les changements de contexte ne suivent pas ce surcoût. Le mécanisme n'est pas identifié.

**SolidEngine (Memory) contre Redis Lua** (sans AOF ; Redis Lua sans délais, retries ni expiration de lease, donc moins de garanties) :

| Config | Cible | Redis Lua | SolidEngine |
|---|---|---|---|
| 2×32 | saturation | 75 k jobs/s, 13,3 µs/job | 175–219 k jobs/s, 8–10 µs/job |
| 2×32 | 50 k | 19,9 µs/job | 28,7 µs/job |
| 2×32 | 80 k | 67 k atteints, 14,8 µs/job | 80 k, 20,6 µs/job |
| 8×8 | saturation | 61 k jobs/s, 16,2 µs/job | 143 k jobs/s, 13,6 µs/job |
| 8×8 | 50 k | 19,6 µs/job | 31,4 µs/job |
| 8×8 | 80 k | 53 k atteints, 18,7 µs/job | 80 k, 22,4 µs/job |

SolidEngine l'emporte en débit maximal (×2,3 à ×2,9, avec ≈ 2 cœurs contre ≈ 1 pour Redis) et tient 80 000 jobs/s là où Redis sature. À charge modérée, Redis consomme moins de CPU par job : **SolidEngine ne le bat pas sur ce critère**. Le CPU/job à saturation n'est pas représentatif d'une charge modérée.

**Réacteur expérimental** (`SOLID_ENGINE_REACTORS=N`, Linux, TCP, désactivé par défaut, `src/reactor.rs`) : N threads epoll multiplexent les connexions à la place d'un lecteur et d'un écrivain par connexion. Pas de délai d'inactivité ; requêtes rares traitées dans un thread dédié ; suite de tests non exécutée avec le drapeau.

| 8×8, µs CPU/job | actuel | 1 réacteur | 2 réacteurs |
|---|---|---|---|
| 50 k jobs/s | 32,3 | 26,5 | 30,6 |
| 80 k jobs/s | 22,9 | 18,9 | 22,2 |
| débit maximal (fenêtre 8) | 139 k | 92 k | 109 k |

Décomposition à 50 k : actuel lecteur 9,8 + écrivain 12,0 + shards 9,5 ; 1 réacteur réseau 16,6 + shards 9,1 ; 2 réacteurs réseau 19,2 + shards 11,0. Le gain du réacteur unique (≈ −18 %) vient du réseau ; avec 2 réacteurs les shards coûtent plus cher (mécanisme non identifié). Le seuil visé (−30 %) n'est pas atteint et le débit maximal recule : le réacteur n'est pas adopté.

Profil du réacteur unique (`SOLID_ENGINE_REACTOR_STATS=fichier`, CPU thread par segment, 8×8, 50 k jobs/s, 3 passes ; l'instrumentation ajoute ≈ 2 µs/job, non soustraits) : `write` 5,1–5,2 µs/job (≈ 1,1 write/job, ≈ 4,7 µs par appel, 2,7 réponses par write), soumission aux shards 4,9, décodage 2,6, `read` 2,2 (≈ 1,15 read/job, ≈ 2,6 trames par read), récupération et encodage des réponses 0,65, `epoll_wait` 0,55 ; 16,0 µs attribués sur 19,2 mesurés avec profil. Chaque job échange 3 requêtes et 3 réponses (push, claim, ack) : les appels système (≈ 7,4 µs) pèsent près de la moitié du chemin réseau et sont peu amortis à cette cadence.

#### Coordination avec les shards et spin — résultat négatif

`strace -f` (débit fortement dégradé, à titre indicatif) : le réacteur émet des `FUTEX_WAKE` (≈ 0,8 par requête) et les shards des `FUTEX_WAIT`, sans `EAGAIN` notable. Sans ptrace, à 50 k jobs/s (8×8, R=1, compteurs) : 3,04 commandes, 1,25 lot et 1,25 réveil effectif par job, soit ≈ 2,4 commandes par réveil ; chaque lot se termine par une attente bloquante. `mpmc::Sender::try_send` ne notifie que si un receveur est enregistré (`SyncWaker::is_empty`) : la notification est déjà conditionnelle.

Spin des shards (`SOLID_ENGINE_SPIN_SHARD_US`), 8×8, R=1, 50 k jobs/s, serveur CPU 6–7, générateur CPU 0–5, 3 passes entrelacées, compteurs activés (leur surcoût, ≈ +1,3 µs/job, rend ces valeurs non comparables aux 25,8 µs de la référence non instrumentée ; on ne compare qu'à l'intérieur de la série) :

| Spin shard | CPU total µs/job | CPU shards µs/job | Réveils/job | p99 |
|---|---|---|---|---|
| 0 µs | 27,1–27,3 | 10,8–11,4 | 1,00–1,14 | 3,6 ms |
| 20 µs | 30,6–30,8 | 14,8–14,9 | 0,75–0,89 | 3,65–3,7 ms |
| 50 µs | 32,7–32,9 | 18,9–19,5 | 0,29–0,36 | 4,0–4,1 ms |
| 100 µs | 34,6–34,7 | 20,9–21,3 | 0,07–0,11 | 6,9–8,6 ms |

Le spin réduit les réveils (jusqu'à −90 %) mais augmente le CPU/job de ≈ 13 %, 21 % et 27 %, et dégrade le p99 à 100 µs. Dans cette configuration (2 CPU, VM), éviter les réveils par attente active coûte plus que les réveils évités ; il reste conservé à 0 (défaut). Cela n'établit pas que les réveils sont sans coût, ni qu'aucune autre réduction de la synchronisation n'est possible. Piste suivante : coalescence des `write` du réacteur (≈ 1,1 write/job, ≈ 5 µs/job), précédée d'une observation sans changement de comportement.

#### Zero-Dispatch (prototype, Memory, 1 shard)

Plancher d'un shard appelé directement (`cargo run --release -p solid-engine-core --example shard_floor`, un thread, sans WAL ni canal, 256 B) : ≈ 0,83 µs/job pour push + claim + ack, contre 9,1–9,7 µs/job de CPU de thread shard mesurés dans le serveur. Environ 8,9 µs/job ne sont donc pas du travail de shard (canal, réveils, callbacks, file de réponses).

Prototype : `SOLID_ENGINE_REACTORS=1 SOLID_ENGINE_REACTOR_DIRECT=1` (Linux, Memory uniquement, 1 shard, désactivé par défaut). Le thread réseau possède son propre `Shard` et exécute push/claim/ack/nack/extend directement, sans canal. Non pris en charge : WAL, claim avec attente (`wait_ms`), partition de claim, requêtes rares, plusieurs shards. Le chemin classique est inchangé quand le drapeau est absent.

8×8, 1 shard (`BENCH_SHARDS=1`), fenêtre 8, médianes de 3 passes entrelacées, serveur CPU 6–7, générateur CPU 0–5, VM Docker Desktop :

| Mesure | R=0 | R=1 | Direct |
|---|---|---|---|
| CPU/job à 50 k | 29,3 | 22,6 | 14,3 |
| CPU/job à 80 k | 20,8 | 16,3 | 10,9 |
| débit saturé | 134,5 k | 130,7 k | 184,8 k |
| CPU/job saturé | 13,4 | 11,1 | 5,3 |
| p50 à 50 k | 443 µs | 357 µs | 138 µs |
| p99 à 50 k | 3,5 ms | 3,6 ms | 3,6 ms |

En 1×64 : 19,2 / 19,3–20,4 / 11,0 µs/job à 50 k. Le p99 à 50 k est dominé par le générateur dans toutes les variantes. Réserves : 1 shard seulement (non comparable aux 143,6 k du 8×8 à plusieurs shards), fonctions absentes qui contribuent peut-être au gain, une seule VM, plusieurs threads propriétaires et routage non testés.

Autres résultats négatifs : un délai de drainage des réponses du réacteur (`SOLID_ENGINE_REACTOR_DRAIN_US`, 0 par défaut) fait tomber le débit à ≈ 19 k jobs/s dès 50 µs (p50 3 ms) ; `perf record -e cpu-clock` dans la VM ne résout pas les symboles noyau (script `benchmark_solid_engine/scripts/native_profile.sh` prêt pour Linux natif, non exécuté).

##### Avec WAL (balanced, strict)

Le mode direct possède aussi le WAL du shard : les réponses sont libérées après le `flush` (balanced) ou après le fsync (strict, depuis le thread WAL). Même protocole : 1 shard, 8×8, serveur CPU 6–7, générateur CPU 0–5, 3 passes entrelacées (R=0 contre direct, Docker Desktop).

| Mode | Mesure | R=0 | Direct |
|---|---|---|---|
| balanced | CPU/job à 50 k | 30,0–30,7 µs | 20,9–21,6 µs |
| balanced | p50 à 50 k | 372–526 µs | 213–235 µs |
| balanced | saturation | 122–133 k/s (13,3–14,3 µs/job) | 146–156 k/s (7,2–7,6 µs/job) |
| strict | CPU/job à 50 k | 25,5–26,4 µs | 12,9–15,0 µs |
| strict | débit atteint (cible 50 k) | 27,7–28,5 k/s | 30,9–34,6 k/s |
| strict | saturation | 31,9–35,1 k/s | 34,5–34,9 k/s |

En strict, le plafond (~35 k jobs/s) est fixé par le fsync, pas par le réseau : seul le CPU/job baisse. Réserves : Docker Desktop, fsync de la VM non représentatif d'un disque natif, claim avec attente et requêtes rares non couverts, suites de tests non exécutées avec le drapeau.

##### Équivalence fonctionnelle (étape 2)

Le mode direct gère désormais `Ping`, `Cancel`, `GetJob`, `GetQueueStats`, ainsi que `ClaimNextJob` avec `wait_ms` et `WaitForJob` : les attentes sont parquées sans bloquer le thread, réveillées au tour de boucle suivant et expirées par deadline. Une attente orpheline (client parti) peut réserver un job, qui revient après expiration du bail (comme sur le chemin classique, at-least-once).

Le socket Unix est servi par le même réacteur que TCP (même traitement des commandes, `reactor::Stream`). Limite : un seul shard. Les tests `integration`, `pipelining`, `durability` (dont le SIGKILL en Strict) et `fault` passent avec `SOLID_ENGINE_REACTORS=1 SOLID_ENGINE_REACTOR_DIRECT=1`, avec 1 shard forcé par les tests ; `claim_affinity` et `durable_ack`, qui exigent plusieurs shards, ne s'y appliquent pas.

Non-régression Memory (1 shard, 8×8, 3 passes) : 14,3–14,4 µs/job et p50 154–182 µs à 50 k ; saturation 154–167 k jobs/s (6,0–6,3 µs/job). Même réserves : Docker Desktop, Redis Lua sans leases ni retries, pas de Linux natif.

Non-régression WAL après l'étape 2 (direct, 3 passes) : balanced 19,9–20,5 µs/job à 50 k, saturation 141–143 k/s (7,8–8,1 µs/job) ; strict 15,5–16,0 µs/job à 50 k, 31,7–32,2 k/s (plafond fsync). Le test `fault` (erreurs d'E/S du WAL) passe en mode direct. Strict est un peu au-dessus des mesures précédentes (12,9–15,0 µs) et la saturation balanced un peu en dessous (146–156 k/s) : écart modeste, à confirmer avec davantage de passes.

##### Plusieurs threads propriétaires (multi-shards)

Avec `SOLID_ENGINE_REACTORS=1 SOLID_ENGINE_REACTOR_DIRECT=1`, un réacteur est créé par shard (le nombre de réacteurs suit `engine.shards`). Le réacteur i possède le shard i et son WAL : un `fsync` d'un shard ne libère jamais la réponse d'un autre. Une commande destinée à un autre shard passe par la boîte aux lettres de son propriétaire (même `Cmd`, même `apply_cmd` que le chemin classique), avec réveil `eventfd` ; la réponse revient au réacteur d'origine. L'`eventfd` doit être vidé avant `Mailbox::apply`, sinon un envoi concurrent est perdu jusqu'au timeout. `durable_ack` et `claim_affinity` passent en direct ; toute la suite passe en classique et en direct (4 shards).

Memory, 8×8, 3 passes entrelacées, `BENCH_PARTITION=1`, `SOLID_ENGINE_CLAIM_AFFINITY=1` :

| Shards | CPU/job à 50 k, R=0 | CPU/job à 50 k, direct | Saturation R=0 | Saturation direct |
|---|---|---|---|---|
| 2 | 29,3–30,2 µs | 21,9–23,3 µs | 129–143 k/s | 169–195 k/s |
| 4 | 31,5–32,1 µs | 22,7–29,2 µs | 132–151 k/s | 146–175 k/s |

Le p50 à 50 k vaut 157–335 µs en direct contre 274–476 µs en R=0 ; le p99 (≈ 3,6 ms) est inchangé.

Réserves : un premier lot avait donné un débit saturé bimodal (18–85 k jobs/s). Il n'a pas été reproduit sur 35 passes ultérieures, et un détecteur de réveil perdu n'a rien signalé ; sa cause reste non établie (bruit de la VM possible). Le gain à 4 shards est plus faible et plus variable qu'à 2. Docker Desktop, pas de Linux natif.

Avec WAL (2 shards, 8×8, 3 passes entrelacées) :

| Mode | CPU/job à 50 k, R=0 → direct | Saturation, R=0 → direct |
|---|---|---|
| Balanced | 31,6–33,2 → 27,9–30,3 µs | 124–128 k → 155–164 k/s |
| Strict | 29–37 → 23–30 µs | 24,1–24,7 k → 24,9–25,6 k/s |

Balanced : gain de CPU d'environ 10 %, débit saturé +25 à 30 %. Strict : le `fsync` de la VM plafonne le débit à ≈ 25 k jobs/s, donc 50 k n'est jamais atteint (21–24 k réels) et les CPU/job ne sont pas comparables à charge égale ; le gain de débit (2 à 5 %) est faible. Le `fsync` Docker Desktop n'est pas représentatif d'un disque natif ; 4 shards en WAL non mesurés.

#### Mémoire (RSS) sous charge

Campagne : 50 k jobs/s, 8×8, 4 shards, 3 passes entrelacées, RSS du serveur relevé **une seule fois en fin d'exécution** (`ps`), VM Docker Desktop.

| Jobs | Redis Lua | SolidEngine classique (R=0) | SolidEngine direct |
|---|---|---|---|
| 300 000 | 16,4 Mo | 109–110 Mo | 102–107 Mo |
| 1 000 000 | 16,1–16,2 Mo | 354–360 Mo | 354–358 Mo |

SolidEngine croît linéairement avec le nombre de jobs traités (≈ 350 octets/job), Redis reste plat ; le mode direct ne change pas l'empreinte. Les ≈ 28 Mo cités plus haut ne valaient que pour un petit nombre de jobs.

Cause : la rétention des jobs terminés (`jobs.completed_retention_ms`, 60 s par défaut) est plus longue que la durée du run (≈ 20 s), donc rien n'est purgé. Test (direct, 1 M de jobs, 3 passes, variable de banc `BENCH_RETENTION_MS`) :

| Rétention | RSS |
|---|---|
| 60 s (défaut) | 348–358 Mo |
| 1 s | 62–63 Mo |

L'empreinte en régime permanent est donc bornée par débit × rétention × ≈ 350 octets, ce qui est un réglage et non une fuite. Même avec 1 s, SolidEngine reste à ≈ 62 Mo contre ≈ 16 Mo pour Redis : la mémoire n'est **pas** un avantage de SolidEngine. Le Redis Lua du banc supprime les jobs terminés, sans leases ni retries.

Réserves : pas de RSS initial ni de pic (une seule mesure en fin de run) ; les ≈ 62 Mo restants (base, buffers, fragmentation, jobs en vol) ne sont pas décomposés ; 50 k jobs/s uniquement ; un seul payload. Prochaine étape : échantillonner le RSS pendant l'exécution et décomposer la mémoire de base.

##### Mémoire : tas de leases périmées (corrigé)

Diagnostic (`SOLID_ENGINE_MEM_STATS=1`, classique, 1 M de jobs, rétention 1 s) : le tas de leases contenait une entrée par job jamais retirée à l'ACK (suppression paresseuse), donc ≈ 250 k entrées par shard jusqu'à l'expiration du bail (30 s), soit ≈ 45 Mo. Correctif dans `shard.rs` : compaction amortie du tas quand il dépasse 2× sa taille après la dernière compaction (+ 4096). Tests du cœur et suite d'intégration verts en classique et en direct.

RSS après 1 M de jobs (50 k jobs/s, 8×8, 4 shards, 3 passes) :

| Rétention | Avant (R=0 / direct) | Après (R=0 / direct) | Redis Lua |
|---|---|---|---|
| 1 s | ≈ 67 / ≈ 62 Mo | 25,3–25,7 / 22,8–23,0 Mo | ≈ 16 Mo |
| 60 s (défaut) | ≈ 354–366 / ≈ 354 Mo | ≈ 303 / ≈ 300 Mo | ≈ 16 Mo |

Reste : avec la rétention par défaut, ≈ 300 octets par job terminé (entrée `Job` de 128 octets avec sa clé, jeu de la table de hachage, entrée du tas de rétention de 32 octets). Redis supprime les jobs à l'ACK ; SolidEngine les garde pour la déduplication et `GetJob`. Pistes non réalisées : table compacte pour les jobs terminaux. Le CPU/job n'a pas été comparé en passes entrelacées avant/après (valeurs à 50 k : R=0 31–32 µs, direct 23–27 µs, dans la fourchette précédente).

##### Mémoire : table compacte des jobs terminés

Un job terminal quitte la table des jobs actifs (`Job`, 128 octets + clé) pour une table `Done` (48 octets utiles : canal, état, priorité, tentatives, fencing, dates, longueur du payload ; sans payload, bail ni réservation). Déduplication, `get`, `cancel`, limite de jobs par shard et validation des réservations lisent les deux tables ; le format de snapshot est inchangé. Seul écart observable : `lease_deadline` vaut 0 dans `GetJob` d'un job terminé. Tests du cœur et suite d'intégration verts en classique et en direct.

RSS (50 k jobs/s, 8×8, 4 shards, 3 passes entrelacées, une mesure en fin de run) :

| Jobs / rétention | Avant (R=0 / direct) | Après (R=0 / direct) | Redis Lua |
|---|---|---|---|
| 300 k / 60 s (défaut) | ≈ 108 / ≈ 104 Mo | 57,7–58,4 / 54,6–55,9 Mo | 16,4 Mo |
| 1 M / 60 s (défaut) | ≈ 303 / ≈ 300 Mo | 175,4–175,6 / 172,5–172,6 Mo | 16,1 Mo |
| 1 M / 1 s | 25,3–25,7 / 22,8–23,0 Mo | 17,2–17,6 / 14,7–15,0 Mo | 16,1 Mo |

Débit tenu à 50 k jobs/s partout ; CPU/job à 50 k : R=0 31,6–32,0 µs, direct 22,7–25,9 µs (fourchette inchangée ; pas de passes avant/après entrelacées pour ce point).

Lecture : avec 1 s de rétention, le mode direct (14,7–15,0 Mo) passe sous Redis Lua (≈ 16 Mo), mais Redis supprime les jobs à l'ACK et ne garde ni déduplication ni `GetJob` après terminaison : comparaison non équivalente. Avec la rétention par défaut (60 s), SolidEngine reste à ≈ 172 Mo après 1 M de jobs (≈ 11× Redis) ; ≈ 170 octets par job retenu (entrée `Done` + jeu de la table de hachage + tas de rétention de 32 octets). Pistes non faites : purge par roue temporelle (supprime le tas), table à adressage ouvert plus dense, rétention par défaut plus courte. RSS mesuré une seule fois en fin de run, VM Docker Desktop.

##### Mémoire : rétention par tranches (au lieu d'un tas)

Le tas de rétention (32 octets par job) est remplacé par des tranches de 128 ms (`BTreeMap<tranche, VecDeque<id>>`, 16 octets par job) ; la date de purge exacte est stockée dans `Done` (sans coût de taille : elle tient dans le padding). Un job n'est jamais purgé avant son échéance ; avec des échéances hors ordre dans une même tranche, la purge peut être retardée d'au plus une tranche. Snapshot et WAL inchangés ; nouveau test d'expiration exacte après restauration ; 15 suites vertes en classique et en direct.

RSS direct (50 k jobs/s, 8×8, 4 shards, rétention 60 s, 3 passes) :

| Jobs | Avant | Après | Redis Lua |
|---|---|---|---|
| 300 k | 54,6–55,9 Mo | 43,1–43,3 Mo | 16,4 Mo |
| 1 M | 172,5–172,6 Mo | 155,5–157,4 Mo | 16,1 Mo |

Gain ≈ −9 % à 1 M de jobs (≈ 157 octets par job retenu) : bien inférieur à une disparition des 172 Mo, la table `Done` et ses jeux de capacité restant. Débit tenu à 50 k jobs/s ; p99 ≈ 3,5 ms inchangé ; CPU/job 23,6–28,8 µs (contre 22,7–25,9 avant) : écart possiblement du bruit, non tranché sans passes avant/après entrelacées. SolidEngine reste ≈ 10× Redis avec la rétention par défaut. Piste suivante, seulement si on poursuit : table à adressage ouvert pour `Done`.
