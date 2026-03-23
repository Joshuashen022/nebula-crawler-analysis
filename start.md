# Crawler Repo Usage

This repository runs a Nebula crawler + monitor and exposes analysis endpoints over HTTP (`:8080`).

## Prerequisites

- Docker + Docker Compose
- Python 3.12+ (for local development)
- Optional: `make`

## 1) Local Python setup (optional, for running without Docker)

```sh
cd ~/code/project/phd/crawler
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file in the repo root (required by the API):

```env
AUTH_TOKEN=change-me
INTERVAL_COUNT=6
```

## 2) Run with Docker (recommended)

Build and start services:

```sh
make build
make up
```

Or directly:

```sh
docker build -t crawler:latest .
docker compose up -d
```

Check status/logs:

```sh
make ps
make logs
```

Stop services:

```sh
make down
```

## 3) API usage

The API runs on `http://localhost:8080` and requires:

```text
Authorization: Bearer <AUTH_TOKEN>
```

Examples:

```sh
# Health/status
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/status

# Start crawl + monitor in background threads
curl -X POST -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/crawl

# Trigger analysis
curl -X POST -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/analyze

# View runtime config
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/config
```

Useful GET endpoints include:
- `/global-geographical`
- `/global-new-found`
- `/global-each-crawl`
- `/protocol-peer`
- `/dbs/protocols`

## 4) Nebula CLI helper scripts

From repo root:

```sh
# Crawl into PostgreSQL + keep API alive
./crawl.sh

# Monitor mode
./monitor.sh

# Resolve mode (ASN/country database lookups)
./resolve.sh
```

## 5) Database and result export

Dump local DB:

```sh
pg_dump -U joshua -d nebula_local > nebula_backup.sql
```

Dump DB from dockerized PostgreSQL:

```sh
docker exec -t nebula-postgres pg_dump -U joshua nebula_local > nebula_backup.sql
```

Copy remote result bundle:

```sh
scp -i ../.ssh/crawler.pem root@8.216.32.203:~/results_bundle.tar ~/Downloads/
```

Remote SSH helper:

```sh
./connect.sh
```