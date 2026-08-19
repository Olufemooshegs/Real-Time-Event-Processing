.PHONY: up down logs health topic-create topic-describe

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

health:
	bash scripts/healthcheck.sh

topic-create:
	docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic transactions.raw --partitions 6 --replication-factor 1

topic-describe:
	docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic transactions.raw
