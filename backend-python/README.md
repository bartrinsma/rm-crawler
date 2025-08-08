# RM SEO Crawl Backend (FastAPI)

## Local run
```
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export AUTH_TOKEN=CHANGE_ME
export WP_ORIGIN=https://jouwdomein.nl
uvicorn app.main:app --reload --port 8080
```

## Docker
```
docker build -t rm-crawler .
docker run -p 8080:8080 -e AUTH_TOKEN=SuperSecret -e WP_ORIGIN=https://jonkerssportprijzen.nl rm-crawler
```

## Health check
Public endpoint: `/healthz` (no auth) for Render/uptime checks.
