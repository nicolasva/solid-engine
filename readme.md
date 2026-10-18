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
- coût mesuré (Docker Desktop, Zero-Dispatch, 4 shards, Memory, 8×8, 50 k jobs/s, 300 k jobs, 3 passes entrelacées) : CPU/job 26,7–29,7 µs sans AUTH contre 27,0–28,8 µs avec AUTH (plages qui se recouvrent, pas de différence mesurable) ; p99 3,4–3,6 ms dans les deux cas. Connexion courte : la boucle d'acceptation dormait `ACCEPT_POLL` = 20 ms, ce qui retardait de ≈ 21 ms la première requête de toute nouvelle connexion (connect 38–40 µs, AUTH 20,6–21,0 ms, avec ou sans AUTH). Elle attend désormais avec `poll()` (réveil immédiat, vérification de l'arrêt toutes les 20 ms) : médianes de 200, classique / Reactor / direct, connect 12–13 µs, AUTH 50–84 µs, Ping 49–92 µs. Saturation non mesurée avec AUTH ;
- un `AUTH` valide et des commandes envoyés dans le même paquet sont traités dans l'ordre ; une commande avant un AUTH valide n'est jamais exécutée (test `tests/auth.rs`).

### TLS via proxy HAProxy (phase 6)

SolidEngine ne chiffre pas lui-même : en réseau non fiable, placer un proxy TLS devant le port TCP, qui doit rester joignable uniquement par le proxy (localhost ou interface privée ; ne pas exposer le port clair). Le jeton `AUTH` ne protège pas contre l'écoute du réseau sans TLS.

Mesure (Docker Desktop, Zero-Dispatch, 4 shards, Memory, 8×8, 50 k jobs/s, 300 k jobs, AUTH actif, 3 passes entrelacées, HAProxy 2.6, certificat RSA 2048 auto-signé, `nbthread 1`). Le banc, en clair, passe par un second HAProxy côté client (CPU 0-5) qui ouvre le TLS ; le HAProxy serveur est épinglé sur les CPU 6-7 comme le moteur. Activation : `BENCH_TLS=1`.

| | Sans TLS | Avec TLS |
|---|---|---|
| Débit | 49 959–49 968 jobs/s | 49 960–49 977 jobs/s |
| p50 | 152–173 µs | 741–747 µs |
| p99 | 3,54–3,62 ms | 3,70–3,86 ms |
| CPU moteur / job | 22,3–24,3 µs | 15,6–16,4 µs |
| CPU HAProxy serveur / job | — | 12,3–12,5 µs |
| CPU total serveur / job | 22,3–24,3 µs | 28,0–28,9 µs |
| Première requête (AUTH) sur connexion neuve | 53–62 µs | 611–622 µs |

Lecture : à 50 k jobs/s le débit est tenu ; TLS ajoute ≈ 6 µs de CPU serveur par job (+ 25 %) et ≈ 0,6 ms de latence médiane (deux sauts de proxy). Le moteur consomme moins de CPU derrière le proxy (≈ −7 µs/job), vraisemblablement parce que le proxy regroupe les paquets (moins d'appels système) ; non vérifié. Le coût d'une connexion neuve passe d'environ 60 µs à 0,6 ms (handshake TLS, mesuré de bout en bout avec les deux proxies).

Saturation (même configuration, sans débit imposé, 400 k jobs, 3 passes entrelacées ; certificat désormais vérifié par le HAProxy client, `verify required` + `verifyhost`) :

| | Débit | p50 | p99 | CPU moteur/job | CPU HAProxy serveur/job |
|---|---|---|---|---|---|
| Sans proxy | 165–217 k jobs/s | 173–287 µs | 1,1–1,35 ms | 9,1–11,6 µs | — |
| HAProxy sans TLS | 99–102 k | 579–621 µs | 0,94–0,98 ms | 10,2–10,7 µs | 5,3 µs |
| HAProxy + TLS | 89–99 k | 604–690 µs | 1,04–1,13 ms | 8,9–10,7 µs | 6,5–7,1 µs |

Lecture : tout proxy (avec ou sans TLS) fait passer le plafond d'environ 165–217 k à environ 100 k jobs/s ; TLS ajoute ≈ 1–2 µs de CPU proxy par job et un débit de 0 à −12 % au-dessus du proxy simple, dans le bruit des passes. **Le plafond TLS n'est pas isolé** : le proxy serveur (un thread) partage les CPU 6-7 avec le moteur, le HAProxy client et les 8 workers partagent les CPU 0-5, et passer le HAProxy client à 4 threads n'a pas amélioré le débit (74–91 k). Une conclusion fiable sur le plafond du chiffrement demande une machine native avec des CPU dédiés au proxy.

Test d'arrêt : `shutdown_returns_promptly_with_no_incoming_connection` (< 500 ms sans connexion entrante).

Réserves : un HAProxy client est dans la boucle de mesure ; certificat auto-signé (vérifié par le proxy client dans la campagne de saturation, non vérifié dans la première table) ;pas de reprise de session ni de TLS 1.3 forcé ; saturation et connexions courtes à haut débit non mesurées ; aucune comparaison avec un TLS natif (rustls).

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

### Diagnostic mémoire des jobs terminés (1 M de jobs, rétention 60 s)

`SOLID_ENGINE_MEM_STATS=1` affiche toutes les 5 s, par shard, `mem_report` (entrées, capacités, taille des structures) et, pour le processus, `mem_process` (RSS, octets alloués/libres/mmappés de glibc). Mesure : Zero-Dispatch, 4 shards, Memory, 50 k jobs/s, 1 M de jobs, 1 passe, RSS final 157,3 Mo.

| Poste (par shard, fin de run) | Valeur | ×4 shards |
|---|---|---|
| Entrées terminées | ≈ 250 000 | ≈ 1 M |
| Capacité de la table `done` | 458 752 emplacements (54 % utilisés) | |
| Emplacement (clé u128 + `Done` de 48 o) | 64 o | |
| Table `done` estimée (capacité × 65 o) | 29,8 Mo | ≈ 119 Mo |
| Rétention (157 tranches, capacité 320 512 ids × 16 o) | 5,1 Mo | ≈ 20,5 Mo |
| Total estimé | ≈ 35 Mo | ≈ 140 Mo |

Processus : RSS 164,9 Mo ; glibc : 136 Mo mmappés (les grandes tables), 22 Mo alloués dans l'arène, 5 Mo libres conservés. L'allocateur ne retient donc presque rien : les ≈ 157 Mo sont des données vivantes, pas de la fragmentation ni une fuite.

Constats (mesurés) :
- ≈ 119 o de table + ≈ 20 o de rétention par job terminé, soit ≈ 140 o sur les ≈ 157 o de RSS par job ;
- la table est à 54 % de remplissage : la capacité d'une table de hachage double par paliers (524 288 emplacements pour 250 000 entrées, seuil de croissance à 229 376). Le RSS fait donc des paliers en fonction du nombre d'entrées retenues par shard ; un autre volume de jobs donnerait un autre rapport octets/job.

Pistes (non implémentées, estimations à valider par mesure) : table à adressage ouvert avec un remplissage plus élevé et sans paliers de doublement ; réduire l'emplacement de 64 à 48 o en comprimant `Done` (horodatages relatifs en u32) ; stocker les id de rétention en u64 de hachage seulement si la déduplication reste exacte (à éviter : risque de collision). Redis Lua ne conserve pas ces entrées ; une comparaison équitable demande un scénario Redis avec une clé par job terminé (`SET job:<id> EX 60`), non fait.

### Redis avec rétention de 60 s (comparaison à garanties plus proches)

Jusqu'ici Redis supprimait le job à l'ACK. Variante `BENCH_REDIS_RETAIN=60` : l'ACK conserve `job:<id>` avec l'état (`s=d`) et la taille du payload (`n`), supprime le payload et la réservation, puis `EXPIRE 60`. La déduplication (`EXISTS`) reste valable pendant 60 s. Mesure : 1 M de jobs, 50 k jobs/s demandés, 8×8, Redis sur CPU 6-7, 3 passes entrelacées (Docker Desktop).

| | Débit réel | p50 | p99 | RSS | CPU/job |
|---|---|---|---|---|---|
| Redis Lua, suppression à l'ACK | 49 984–49 987 jobs/s | 639–661 µs | 3,4–3,65 ms | 16,2 Mo | 19,6–19,7 µs |
| Redis Lua, trace 60 s | 45 358–46 671 | 1,31–1,35 ms | 2,1–2,2 ms | 450,5 Mo | 21,4–22,1 µs |
| SolidEngine direct, 4 shards, 60 s | 49 987 | 178–202 µs | 3,5–3,6 ms | 157,3–157,5 Mo | 25,3–28 µs |

Lecture : avec une trace conservée 60 s, Redis utilise ≈ 2,9× plus de RAM que SolidEngine (450 contre 157 Mo) et ne tient pas les 50 k jobs/s demandés (45–47 k). En revanche son CPU/job est plus bas (≈ 21–22 contre 25–28 µs). Redis seul reste à 16 Mo s'il supprime à l'ACK.

Réserves : la trace Redis (hash de 2 champs + expiration) ne reproduit pas tout `GetJob` (canal, priorité, tentatives max, fencing, horodatages) ; le nombre de clés a été vérifié sur deux passes supplémentaires, juste après la charge : `DBSIZE` = 1 000 002 (1 M de traces + 2 clés de compteurs), `used_memory` ≈ 450,1 Mo (≈ 450 o par trace), TTL d'une clé tirée au hasard = 38 et 50 s (> 0, aucune expiration prématurée) ; côté SolidEngine, `mem_report` totalise ≈ 999 800 jobs terminés retenus ; le RSS n'est mesuré qu'en fin de run ; Docker Desktop, pas de Linux natif ; Redis monothread contre 4 shards SolidEngine.

### Emplacement de la table `done` : 64 → 56 octets

Un `Done` faisait 48 o sans padding récupérable (4 × u64, 3 × u32, u16, 2 × u8) ; avec une clé u128 (alignement 16) l'emplacement valait 64 o. Deux changements sans perte :
- la clé est un `[u64; 2]` (alignement 8) ;
- `created_at` et `scheduled_at` sont stockés comme écarts u32 sous `purge_at` (`Done` = 40 o). Quand un écart dépasse `u32::MAX − 1` ms (≈ 49,7 jours) ou que l'horodatage est postérieur à `purge_at` (job planifié très tard puis annulé), les deux valeurs exactes vont dans une table annexe `wide` (libérée à la purge) : aucune perte de précision, snapshots et WAL inchangés.

Tests : `done_slot_is_56_bytes`, bornes de l'écart (`u32::MAX − 1` compact, `u32::MAX` annexe), jobs hors plage exacts via `get`, snapshot/restauration et purge (14 tests du cœur, 16 suites dans les trois modes).

Mesure (Zero-Dispatch, 4 shards, Memory, 50 k jobs/s, 1 M de jobs, rétention 60 s, 3 passes) : emplacement 56 o confirmé par `mem_report` (capacité de table inchangée : 458 752).

| | Avant | Après |
|---|---|---|
| RSS | 157,3–157,5 Mo | 135,6–136,1 Mo (−13,5 %) |
| Débit | 49 987 jobs/s | 49 987–49 993 |
| p50 / p99 | 178–202 µs / 3,5–3,6 ms | 148–173 µs / 3,58 ms |
| CPU/job | 25,3–28 µs | 22,4–24,1 µs |

Réserves : pas d'A/B entrelacé (l'ancien binaire n'a pas été conservé) : le CPU/job est comparé aux campagnes précédentes, variables de ±15 % ; l'économie mesurée (≈ 21,5 Mo) dépasse la projection de 15 Mo, sans explication vérifiée ; le nombre d'entrées dans `wide` sur ce banc n'a pas été relevé (attendu : 0). Contre Redis avec trace 60 s (450 Mo) : ≈ −70 %, même comparaison non équivalente qu'avant.

### Table `done` dense (feature `dense-done`)

Table à adressage ouvert (`done_table.rs`) : sondage linéaire, suppression par décalage (sans pierres tombales), plafond de remplissage 3/4, croissance ×1,25 (remplissage 60–75 % contre ≈ 54 % en moyenne pour hashbrown), réduction de plage sans modulo. La clé nulle marque un emplacement libre ; l'identifiant 0 est stocké à part. Elle n'est plus le défaut depuis `split-done` (voir plus bas) ; `--no-default-features` donne le `HashMap` standard. Il n'existe pas de bascule au démarrage.

Tests : comparaison avec `HashMap` sous 200 000 insertions/suppressions sur 3 000 clés (grappes, enroulements), identifiant 0 et réduction ; les 16 suites passent avec la feature dans les trois modes.

A/B entrelacé, 4 passes, Zero-Dispatch, 4 shards, 50 k jobs/s, 1 M de jobs, rétention 60 s (feature `slot64` : clé alignée sur 16 → emplacement de 64 o, pour isoler l'effet de l'emplacement) :

| Variante | RSS | CPU/job | p50 | p99 |
|---|---|---|---|---|
| hashbrown, 64 o | 157,1–157,6 Mo | 22–27 µs | 149–210 µs | 3,53–3,62 ms |
| hashbrown, 56 o | 135,9–136,2 Mo | 22–26 µs | 148–203 µs | 3,53–3,59 ms |
| dense, 64 o | 120,3–120,7 Mo | 22–28 µs | 158–211 µs | 3,52–3,61 ms |
| dense, 56 o | 103,1–106,0 Mo | 22–27 µs | 156–212 µs | 3,53–3,62 ms |

Débit tenu (49 971–49 990 jobs/s) partout. Aucune différence de CPU ou de latence au-delà du bruit (±15 %) ; l'emplacement de 56 o est donc mesuré sans coût CPU contre 64 o. Gain dense : ≈ −24 % de RSS (136 → 103–106 Mo). Face à Redis avec trace 60 s (450 Mo) : ≈ −77 %.

Réserves : RSS relevé en fin de run, donc dépendant du remplissage de la table à cet instant (le pic de rehash et la position par rapport au seuil de croissance varient avec le nombre de jobs) ; les pics de latence de rehash (table ×1,25, plus fréquents que ×2) ne sont visibles qu'au p99.9 et n'ont pas été mesurés ; la rétention par tranches n'a pas été comparée en A/B (ancienne version non conservée).

Validation avant activation par défaut :
- Latences d'opérations (test manuel `cargo test --release -p solid-engine-core latency -- --ignored --nocapture`, 2 M d'insertions avec fenêtre glissante de 250 000 entrées) : insertion p99.9 4,5 µs (hashbrown) contre 0,29 µs (dense) ; p99.99 10 contre 7,9 µs ; pause maximale 13,3 ms contre 3,3 ms.
- Bout à bout, 4 passes entrelacées hashbrown 56 / dense 56 : RSS 135,8–136,2 contre 103,2–105,7 Mo ; p99.9 4,6–4,7 contre 4,6–4,8 ms ; débit tenu ; CPU/job 20–22 contre 19–23 µs (indiscernables).
- Rehash dense (`mem_report`, par shard, 1 M de jobs) : 34 à 40 rehashs, pause maximale 1,2 à 10,9 ms, total 8 à 37 ms sur ≈ 20 s ; longueur maximale de sondage 124–171.
- Tests : comparaison avec `HashMap` sous churn, invariant de sondage proche de 75 % après suppressions en grappe, identifiant 0 ; 16 suites vertes dans les trois modes.

Réserves : une pause de rehash allant jusqu'à ≈ 11 ms bloque le shard concerné (hashbrown a des pauses du même ordre, 13 ms mesurées en test unitaire, mais non instrumentées bout à bout) ; elle n'apparaît pas au p99.9 de ce banc ; le sondage maximal de 171 est un pic isolé (p99.99 d'insertion 7,9 µs) mais montre que le remplissage de 75 % crée de longues grappes ; pas de test de churn de plusieurs heures.

Churn prolongé (dense, direct, 4 shards, 50 k jobs/s, rétention 60 s, 20 M de jobs ≈ 6,7 min, ≈ 750 000 jobs retenus par shard en régime établi) : débit 49 990 jobs/s ; p50 182 µs, p99 3,58 ms, p99.9 4,6 ms ; CPU/job ≈ 25 µs. RSS : 109 Mo à 20 s, 325 Mo à ≈ 60 s, puis plateau à 326–331 Mo (+6 Mo sur 5 min, `malloc_free_held` qui monte de 4 à 9 Mo) ; aucun rehash après stabilisation (44 → 45, pas d'oscillation de la capacité : 870 133 pour 750 000 entrées). Pause de rehash maximale : 31–35 ms au franchissement de ≈ 700 k entrées par shard (contre 1–3 ms à 250 k) : elle croît à peu près linéairement avec la taille. Longueur de sondage maximale : 132 → 163–234 sur 5 min.

Limites : seulement ≈ 7 min, pas 2 h ; pas de latences par fenêtre de 30 s (le banc n'agrège qu'en fin de run) ; pas de profil de charge variable ; pause de rehash de hashbrown à cette taille non mesurée. Une pause de ≈ 35 ms suspend un shard (≈ 400 requêtes en attente à 12,5 k jobs/s) : invisible au p99.9, probablement visible au p99.99. Si cela compte, pistes : rehash incrémental, ou abaisser le plafond à 70 %.

#### Variante `split-done` (activée par défaut)

La table est répartie en 64 sous-tables indépendantes (partition par le hash, position par le hash tourné) : un rehash ne déplace qu'une sous-table. Pas de migration progressive ni de double recherche. Repli à la compilation : `--no-default-features --features dense-done` (table dense non partitionnée) ou `--no-default-features` (`HashMap`).

| | dense | split-64 |
|---|---|---|
| Pause de rehash max, 1 M de jobs (3 passes) | 2,8–8,2 ms | 0,22–0,76 ms |
| Pause de rehash max, churn 20 M (≈ 750 000 entrées/shard) | 31–35 ms | 0,6–0,75 ms |
| Rehashs par shard (1 M / 20 M) | 40 / 45 | 1 344 / 1 664 |
| Temps cumulé de rehash par shard, 20 M | ≈ 35 ms (1 M : 8–37 ms) | ≈ 110 ms |
| RSS, 1 M de jobs | 103–105 Mo | 114–116 Mo (+10 %) |
| RSS, churn 20 M, plateau | 326–331 Mo | 298–314 Mo |
| CPU/job, 1 M | 18,6–21,4 µs | 20,9–21,2 µs |
| p99.9, 1 M | 4,8–5,0 ms | 4,55–4,78 ms |
| p50 / p99 / p99.9, churn 20 M | 182 µs / 3,58 ms / 4,6 ms | 162 µs / 3,59 ms / 4,62 ms |

Le surcoût mémoire de 10 % à 1 M de jobs ne se retrouve pas à 750 000 entrées par shard (hypothèse : arrondi des tailles de sous-tables à croissance ×1,25, non vérifiée par un relevé séparé). Le travail total de rehash est ≈ 3× plus élevé mais étalé en 1 600 petites pauses. Le p99.99 d'insertion se dégrade au niveau unitaire (7,9 → 38,8 µs : les 1 346 petits rehashs pèsent plus de 0,01 % des insertions). Longueur de sondage maximale : 160–203, comme en dense. Tests : équilibre des sous-tables pour identifiants séquentiels ou à bits constants (écart max ≤ 10 %), comparaison avec `HashMap`, 16 suites vertes dans les trois modes avec la feature.

Vérification du binaire par défaut (4 passes entrelacées, 1 M de jobs, dense non partitionné / split-64) : débit 49 980–49 989 jobs/s ; p50 147–199 / 156–194 µs ; p99.9 4,62–4,65 / 4,54–4,58 ms ; RSS 103–105 / 113–115 Mo ; CPU/job 23–27 / 23–26 µs. Le p50 de 290 µs et le p99.9 de 5,05 ms vus sur un run isolé n'étaient pas reproduits (bruit).

Rétrécissement : `shrink_to` ne réduit qu'une sous-table par `tick` (pour borner la pause) ; le shard n'ayant plus d'échéance après la dernière purge, `next_deadline` renvoie `dernier tick + 1 ms` tant qu'une réduction est en attente, sinon les sous-tables restantes seraient restées grandes jusqu'à la prochaine activité (défaut trouvé en préparant l'activation par défaut). Test `done_table_shrinks_completely_after_large_purge` : 100 000 jobs purgés, capacité finale ≤ 4 096. La durée d'une réduction n'est pas mesurée séparément (c'est un rehash d'une sous-table, de l'ordre des pauses de rehash mesurées) ; RSS rendu à l'OS non mesuré après purge ; un seul run de 20 M par variante ; pas de latences par fenêtre ni de p99.99 bout à bout.

#### Mémoire après purge (split-done, 3 cycles dans le même processus)

`BENCH_PURGE_CYCLES=3 BENCH_PURGE_WAIT=75` : 1 M de jobs à 50 k/s, puis 75 s sans aucune commande (rétention 60 s), trois fois dans le même serveur (direct, 4 shards). Démarrage : RSS 35 Mo.

| Cycle | RSS après remplissage | RSS après 75 s sans commande | Mémoire allouée (`malloc_in_use`) après purge | Libre retenu par glibc |
|---|---|---|---|---|
| 1 | 115,7 Mo | 37,8 Mo | 2 Mo | 33 Mo |
| 2 | 123,8 Mo | 62,1 Mo | 2 Mo | 57 Mo |
| 3 | 126,1 Mo | 59,8 Mo | n.r. | n.r. |

- La table `done` revient à 0 entrée et à sa capacité minimale (3 072 emplacements par shard, 229 Ko) sans aucune activité, dans chaque cycle : le correctif de `next_deadline` fonctionne bout à bout.
- La mémoire allouée est identique d'un cycle à l'autre (≈ 96 Mo en fin de remplissage, 31 + 65 Mo mmappés au cycle 1, 96 Mo dans l'arène au cycle 2) : pas d'accumulation de la part du moteur.
- Le RSS, lui, dérive : +8 Mo (cycle 2) puis +2,3 Mo (cycle 3) en fin de remplissage ; 38 → 62 → 60 Mo au repos. Cause visible dans `mallinfo2` : au cycle 2 glibc a relevé son seuil de mmap, les tables vont dans l'arène et 25 puis 57 Mo y restent libres mais non rendus à l'OS. C'est le comportement de l'allocateur, pas du moteur ; seulement 3 cycles, la stabilisation n'est pas démontrée.
- Réduction des sous-tables : 2 600 à 5 300 réductions par shard et par cycle (la réduction progresse par paliers pendant l'expiration) ; pause maximale d'une réduction 0,3–2,6 ms.
- Pistes (non implémentées) : `malloc_trim(0)` à la fin d'une réduction, ou fixer le seuil de mmap.
- Non vérifié : l'arrêt du serveur pendant une réduction en attente.

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
