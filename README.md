# PhD Assignment: IPFS Network Analysis
**Applicant: Joshua Shen** **Target Position: PhD in Network Security, Chalmers University of Technology**

---

## 📄 Final Report
**The comprehensive analysis report can be found here:** 👉 **[report.pdf](./report/report.pdf)** *(Please click the file above to view the findings, visualizations, and methodology.)*

---

## 🔗 Submission Summary
- **Primary Dataset (Local/Experimental):** [QmTepp4mfnTRAfTjW2ytV1VoSv9hE8tLDUDDpb3nby6CWr](https://bafybeico6jh7tqh4iavib73lv6mgk263vq4wmb727frskv34okdgwt2lc4.ipfs.dweb.link?filename=results_local.tar.gz)
- **Baseline Dataset (Server/Control):** [QmTAGj3FAJVMbpVbCuRXwJGX1r5wJLX6kDxvwDy8cGwVN3](https://bafybeichuhco23no4oeaqc25kti6vgj4mi2nvv3wu5qffi4wm6om5uv5wq.ipfs.dweb.link?filename=results_server_server.tar)
- **Focus:** Peer connectivity and NAT traversal challenges in residential network environments.


# Crawler Repo Usage

This repository runs a Nebula crawler + monitor and exposes analysis endpoints over HTTP (`:8080`).

## Prerequisites
- ./dist/nebula: a compiled nebula file under a file from `https://github.com/dennis-tra/nebula`
- Docker + Docker Compose
- Python 3.12+ (for local development)
- Optional: `make`


## 1) Local Python setup (optional, for running without Docker)

```sh
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

## 4) Analysis usage (`src/analysis`)

Analysis logic is implemented in `src/analysis` and exposed via the API.

Run analysis from the API:

```sh
# Trigger resolve + analysis data refresh
curl -X POST -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/analyze
```

Then fetch analysis outputs:

```sh
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/global-geographical
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/global-new-found
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/global-peer-neighbour
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/protocol-peer
curl -H "Authorization: Bearer $AUTH_TOKEN" "http://localhost:8080/protocol-distribution-country?country=US"
curl -H "Authorization: Bearer $AUTH_TOKEN" http://localhost:8080/agent-peer-count
```

Run analysis modules directly (local debug):

```sh
source .venv/bin/activate
PYTHONPATH=. python -c "from src.analysis import global_geographical; print(global_geographical.fetch_geographical_data())"
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
scp -i ../.ssh/crawler.pem root@<remoteIP>:~/results_bundle.tar ~/Downloads/
```