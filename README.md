# S5 DMR Monitor

Docker Compose dashboard for the Slovenian BrandMeister talkgroups **TG 293** and **TG 29320**.

## Quick start

```bash
git clone https://github.com/dguttierrez/s5-dmr-monitor.git
cd s5-dmr-monitor
cp .env.example .env
mkdir -p data
sudo chown 10001:10001 data
docker compose up -d --build
docker compose logs -f --tail=100
```

The web UI is bound to `http://127.0.0.1:8088` by default; place it behind an HTTPS reverse proxy (e.g. Traefik) for remote access.

## How it works

The collector listens to the BrandMeister Last Heard Socket.IO event stream continuously, accepts only completed transmissions for talkgroups 293 and 29320, and stores metadata (not audio) in SQLite. The browser refreshes the dashboard every 45 seconds. Data accumulates **only from the moment the service starts** and is retained for 30 days by default. Repeated stream events are deduplicated. This is not a complete archival history.

Endpoints: `/`, `/health`, `/api/status`, `/api/activity?tg=293&limit=100`, `/api/stats`.

## Environment

Copy `.env.example` to `.env` and edit `HTTP_PORT`, `BIND_IP`, `TALKGROUPS`, `REFRESH_SECONDS`, `RETENTION_DAYS`, `BM_URL`, or `BM_SOCKET_PATH` as necessary. Keep the local `data/` directory persistent across container updates.

## Diagnostics

```bash
docker compose ps
docker compose logs --tail=200 -f
curl -fsS http://127.0.0.1:8088/health
curl -fsS http://127.0.0.1:8088/api/status
curl -fsS http://127.0.0.1:8088/api/activity
```

A green connection status means the Socket.IO stream is connected; it does **not** guarantee BrandMeister is delivering useful events. Observe the logs and actual activity. Socket.IO payload structure and subscription semantics come from community Last Heard clients and may change on the upstream side. This version has not been verified against the live upstream service in a running container.

For local tests install requirements and run `python -m unittest discover -s tests`.

## Security

The web app exposes no admin interface. Keep its bind address at localhost unless protected by a reverse proxy, use TLS for public traffic, and do not commit any private configuration files or secrets. The image uses a non-root user, read-only filesystem, dropped capabilities, and a persistent SQLite volume.

73 de S51DG.
