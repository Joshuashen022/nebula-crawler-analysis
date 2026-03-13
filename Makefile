IMAGE_NAME ?= crawler:latest

.PHONY: build up down logs ps restart

build:
	docker build -t $(IMAGE_NAME) .

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

restart: down up
