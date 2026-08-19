.PHONY: up down logs health topics-apply topic-create topic-describe

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

health:
	bash scripts/healthcheck.sh

topics-apply:
	bash scripts/apply-topics.sh infrastructure/kafka/topics.yml

topic-create: topics-apply

topic-describe:
	docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --describe --topic transactions.raw
