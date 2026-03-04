.PHONY: build run test lint clean inspect

build:
	docker compose build

run:
	docker compose run --rm scraper

inspect:
	docker compose run --rm scraper inspect

test:
	docker compose run --rm --entrypoint "" scraper pytest -q

lint:
	docker compose run --rm --entrypoint "" scraper flake8 .

clean:
	rm -f data/nicodic.db
