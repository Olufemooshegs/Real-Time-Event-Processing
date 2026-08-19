# Real-Time Event Processing & Analytics Platform

## Current scope

This repository currently runs only Kafka and Postgres:

- Kafka is a single-broker KRaft cluster.
- Postgres is initialized with a minimal `transactions.events` connectivity shell.

Flink, event producers, the FastAPI service, ClickHouse, and monitoring are not built yet.

KRaft is used instead of Zookeeper because this is a simple single-broker development setup. There is no operational reason to introduce Zookeeper at this scale.

Kafka topics use replication factor 1. This is a deliberate development-only limitation: one broker cannot tolerate a broker failure or provide replicated durability. It must be increased alongside a multi-broker deployment before production use.

## Run

Create your local environment file, then start the services:

```bash
cp .env.example .env
make up
make health
```

A passing health check prints three `PASS` lines confirming that the Kafka broker accepts connections, a Kafka topic can be created and described, and Postgres accepts a connection.

To create the development topic used in later steps:

```bash
make topic-create
make topic-describe
```

`transactions.raw` has six partitions and replication factor 1.

## Commands

```bash
make up             # start Kafka and Postgres
make down           # stop services
make logs           # follow service logs
make health         # run broker, topic, and database checks
```
