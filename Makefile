.PHONY: build run test lint clean inspect

build:
	docker compose build

run:
	docker compose run --rm scraper

inspect:
	docker compose run --rm scraper inspect

test:
	pytest

lint:
	flake8 .

clean:
	rm -f data/nicodic.db

