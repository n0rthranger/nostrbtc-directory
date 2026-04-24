# nostrbtc Directory

A personalized Web of Trust directory for Nostr, powered by [GrapeRank](https://github.com/NosFabrica/brainstorm_graperank_algorithm) and [NIP-85](https://github.com/nostr-protocol/nips/blob/master/85.md).

**Live deployment:** [https://nostrbtc.com/directory](https://nostrbtc.com/directory)

## What It Does

- **Personalized trust scoring** — Every directory member gets a per-observer GrapeRank trust score from the follow graph, with mutes and reports as negative signals. Two different users can see different rankings.
- **NIP-85 Trusted Assertion publishing** — The relay publishes kind 30382 rank assertions, making Web of Trust data available to any Nostr client that knows the observer's service-provider pubkey.
- **Member discovery** — Given an npub, recommends directory members the user doesn't yet follow, ranked by trust score.
- **Follow recommendations** — Suggests who to follow based on the trust graph and community structure.
- **Decentralized directory** — Directory membership is published as kind 9998/9999 events for censorship-resistant, cryptographically verifiable listings.
- **Social clustering** — Detects communities within the directory using trust graph analysis.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Frontend                             │
│   directory.html + directory.js + dir-terminal.js           │
│   (browser — calls /api/directory/* endpoints)              │
└──────────────┬──────────────────────────────────────────────┘
               │ HTTP
┌──────────────▼──────────────────────────────────────────────┐
│                   Backend (FastAPI)                          │
│   main.py — serves directory API endpoints                  │
│   db.py — Postgres queries + Redis cache                    │
│   directory_indexer.py — background profile pre-computation │
│   discovery.py — multi-relay profile fetching               │
│   attestation.py — NIP-05 and Lightning verification        │
│   clustering.py — social graph community detection          │
│   decentralized_list.py — kind 9998/9999 publishing         │
└──────┬──────────┬──────────┬────────────────────────────────┘
       │          │          │
       ▼          ▼          ▼
   Postgres     Redis      strfry
   (profiles,   (cache,    (Nostr relay,
    trust,       queues)    event store)
    scores)        │
                   │ message queue
       ┌───────────▼───────────┐
       │   Indexer Worker      │
       │   worker.py           │
       │   - tails strfry WS   │
       │   - syncs follow      │
       │     graph to Neo4j    │
       │   - triggers GrapeRank│
       │   - publishes NIP-85  │
       │     kind 30382 events │
       └───────────┬───────────┘
                   │
          ┌────────▼────────┐
          │  Neo4j          │
          │  (social graph) │
          └────────┬────────┘
                   │
       ┌───────────▼───────────┐
       │  GrapeRank (Java)     │
       │  - reads graph from   │
       │    Neo4j              │
       │  - computes per-      │
       │    observer scores    │
       │  - writes results     │
       │    back to Postgres   │
       └───────────────────────┘
```

### Data Flow

1. **strfry** stores Nostr events (profiles, follows, zaps, reactions, etc.)
2. **Indexer worker** tails strfry's WebSocket, extracts social signals, and syncs the GrapeRank graph (follows, mutes, reports) to **Neo4j**
3. Every 6 hours, the indexer triggers a **GrapeRank** computation via Redis message queue
4. **GrapeRank (Java)** reads the social graph from Neo4j, computes per-observer trust scores, and writes results to **Postgres**
5. The **directory indexer** (inside the backend) pre-computes profile data every 15 minutes: NIP-05 verification, Lightning reachability, badges, and trust statistics
6. The **backend** serves all directory API endpoints, reading from Postgres and Redis
7. The indexer publishes **NIP-85 kind 30382** events back to strfry. Each observer uses a dedicated service-provider key so addressable rank events do not collide across observers.

> Production note: nostrbtc.com now runs the GrapeRank computation directly from Postgres in Python. This public repository still includes the Java/Neo4j worker as an open-source batch-worker option, but the standalone Python reference and NIP-85 event shape are kept aligned with the production nostrbtc.com implementation.

## Prerequisites

- **Python 3.12+** — backend and indexer
- **Java 21+** (or Docker) — GrapeRank worker
- **PostgreSQL 15+** — profile and trust score storage
- **Redis 7+** — caching, message queue, rate limiting
- **Neo4j 5+** — GrapeRank graph (follow/mute/report edges)
- **strfry** — Nostr relay (event store and WebSocket server)
- **Podman** or **Docker** — container orchestration

## Setup

### 1. Configure Environment

```bash
cp .env.example .env
# Edit .env with your values — see comments in .env.example for each variable
```

### 2. Initialize the Database

```bash
psql -U your_user -d your_database -f postgres/schema.sql
# Optional: apply directory tier migration
psql -U your_user -d your_database -f postgres/migrate-directory-tier.sql
```

### 3. Start Infrastructure Services

Using Podman/Docker, start the required services:

```bash
# Redis
podman run -d --name directory-redis -p 6379:6379 docker.io/library/redis:7-alpine \
  redis-server --requirepass "$REDIS_PASSWORD"

# PostgreSQL
podman run -d --name directory-postgres -p 5432:5432 \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB=directory \
  docker.io/library/postgres:16-alpine

# Neo4j
podman run -d --name directory-neo4j -p 7687:7687 \
  -e NEO4J_AUTH="neo4j/$NEO4J_PASSWORD" \
  docker.io/library/neo4j:5-community

# strfry (build from Containerfile or use your existing relay)
```

### 4. Build and Start Application Services

```bash
# Build service images from the repository root so shared/nostr_crypto.py is
# copied into each image as nostr_crypto_shared.py.

# Backend
podman build -t directory-backend -f backend/Containerfile .
podman run -d --name directory-backend --env-file .env directory-backend

# Indexer
podman build -t directory-indexer -f indexer/Containerfile .
podman run -d --name directory-indexer --env-file .env directory-indexer

# GrapeRank Java worker
cd graperank-java
podman build -t directory-graperank -f Dockerfile .
podman run -d --name directory-graperank --env-file ../.env directory-graperank
```

### 5. Serve the Frontend

Point a web server (Caddy, nginx, etc.) at the `frontend/` directory and proxy `/api/*` requests to the backend on port 8080.

## NIP-85 Event Format

The relay publishes NIP-85 kind 30382 (trusted assertion) rank events. NIP-85 ranks are integer values from 0 to 100, so raw GrapeRank scores are converted with `ceil(score * 100)` and clamped to `0..100`.

Personalized scores need one service-provider pubkey per observer. In this repository those keys are stored in `service_provider_keys`; the event author is the observer's service-provider pubkey, and the `d` tag is the target pubkey being ranked.

```json
{
  "kind": 30382,
  "pubkey": "<observer-service-provider-pubkey>",
  "created_at": 1712345678,
  "tags": [
    ["d", "<target-pubkey-hex>"],
    ["rank", "27"],
    ["p", "<target-pubkey-hex>"]
  ],
  "content": "",
  "sig": "..."
}
```

### Tag Descriptions

| Tag | Fields | Description |
|-----|--------|-------------|
| `d` | `<target-pubkey>` | The pubkey being ranked |
| `rank` | `<0-100 integer>` | NIP-85 rank derived from the raw 0-1 GrapeRank score |
| `p` | `<target-pubkey>` | Relay hint tag for clients that index by pubkey tag |

Clients can query `{"kinds": [30382], "authors": ["<observer-service-provider-pubkey>"]}` for one observer's rank assertions, or add `"#d": ["<target-pubkey>"]` for one target. Historical malformed assertions can be removed with NIP-09 kind 5 deletion events signed by the same service-provider key.

## API Endpoints

### Directory Listing

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/directory` | Paginated directory listing with optional filters |
| `GET` | `/api/directory/stats` | Aggregate directory statistics |
| `GET` | `/api/directory/tags` | All member tags with counts |
| `GET` | `/api/directory/list-header` | Kind 9998 decentralized list header ID |

### Recommendations & Discovery

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/directory/recommendations/{npub}` | Personalized follow recommendations |
| `POST` | `/api/directory/compute-trust` | Compute GrapeRank scores for a specific observer |

### Trust & Graph Analysis

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/directory/trust-lookup?observer=X&target=Y` | Trust score between two pubkeys with explanation |
| `GET` | `/api/directory/trust-lookup/signals?observer=X&target=Y` | Raw relationship and interaction signals for a lookup |
| `GET` | `/api/directory/trust-graph?observer=X` | Full trust subgraph for visualization |
| `GET` | `/api/directory/trust-path?from=X&to=Y` | Shortest trust path between two pubkeys |
| `GET` | `/api/directory/trust-stats` | Global trust distribution statistics |
| `GET` | `/api/directory/trust-history/{identifier}` | Historical trust score changes |
| `GET` | `/api/directory/clusters` | Social community clusters |
| `GET` | `/api/directory/zap-flow` | Zap flow network between members |

### Activity & Profiles

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/directory/activity/{identifier}` | Activity heatmap for a member |
| `GET` | `/p/{identifier}` | Profile card page (HTML, supports npub or NIP-05) |
| `GET` | `/.well-known/nostr.json?name=X` | NIP-05 resolution |

### Member Management (Authenticated)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/directory/status/{npub}` | Check directory listing status |
| `POST` | `/api/directory/toggle/{npub}` | Toggle directory visibility |
| `POST` | `/api/directory/tags/{npub}` | Update member tags |
| `POST` | `/api/directory/reindex/{npub}` | Trigger re-indexing for a member |

## GrapeRank Algorithm

GrapeRank computes personalized trust scores using a capped weighted average over the social graph. Key parameters:

- **Attenuation** (0.85) — Trust decays as it propagates through the graph
- **Rigor** (0.5) — Controls how much evidence is needed before trusting a score
- **Observer confidence** (0.5) — The observer's self-trust seed value
- **Follow confidence** (0.03) — Non-observer follows are weak evidence
- **Mute/report rating** (-0.1) — Negative signals reduce score when coming from trusted raters

The algorithm is sybil-resistant: a cluster of fake accounts all following each other cannot inflate their scores because the trust must originate from the observer's direct connections.

Two implementations are included:
- **Python** (`indexer/graperank.py`) — Standalone reference implementation aligned with nostrbtc.com production tests
- **Java** (`graperank-java/`) — Batch-worker option derived from the upstream algorithm

## Credits

The trust computation in this project uses the GrapeRank algorithm, designed by David Strayhorn (straycat).

- David Strayhorn: https://ditto.pub/npub1u5njm6g5h5cpw4wy8xugu62e5s7f6fnysv0sj0z3a8rengt2zqhsxrldq3
- Twitter/X: https://x.com/davidstrayhorn
- GrapeRank algorithm source: https://github.com/NosFabrica/brainstorm_graperank_algorithm
- NosFabrica: https://nosfabrica.com

## License

This project uses a dual-license structure:

**MIT** — Everything except `graperank-java/` and the two Python files noted below. See [LICENSE](LICENSE).

**AGPL-3.0** — The `graperank-java/` directory is a fork of [NosFabrica/brainstorm_graperank_algorithm](https://github.com/NosFabrica/brainstorm_graperank_algorithm) by David Strayhorn, licensed under AGPL-3.0. The Java code retains the original class names, algorithm logic, and constant definitions, with performance improvements (graph caching, worklist convergence, connection pooling) added on top. Under AGPL-3.0, any derivative of this code must carry the same license. See [graperank-java/LICENSE](graperank-java/LICENSE).

The Python files `indexer/graperank.py` and `indexer/interpreter.py` carry AGPL-3.0 attribution headers as they are derived from the same upstream project.

If you use only the MIT-licensed components (backend, indexer worker, frontend, schema) and supply your own trust computation engine, no AGPL obligations apply.
