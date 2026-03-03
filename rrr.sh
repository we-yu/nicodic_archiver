#!/bin/bash
# rrr = refresh, run, read
sudo rm data/nicodic.db
docker compose build
docker compose run --rm scraper "https://dic.nicovideo.jp/a/プロイセン(APヘタリア)"
docker compose run --rm scraper inspect 4470620 a --last 10
