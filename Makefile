.PHONY: demo demo-quick test build up down

# 10,000 changes with 20% duplicate, 5% dropped and reordered webhooks.
# Needs Go and Postgres (DATABASE_ADMIN_URL, default local).
demo:
	bench/run.sh

demo-quick:
	EVENTS=2000 OBJECTS=200 bench/run.sh

test:
	go vet ./... && go test -race ./...

build:
	go build -o bin/ ./cmd/sync ./cmd/sim ./cmd/chaos

up:
	docker compose up --build --abort-on-container-exit --exit-code-from chaos

down:
	docker compose down -v
