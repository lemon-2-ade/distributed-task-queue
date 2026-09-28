.PHONY: up down logs ps fmt lint typecheck test

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

fmt:
	ruff format .
	ruff check --fix .

lint:
	ruff check .

typecheck:
	mypy .

test:
	pytest
