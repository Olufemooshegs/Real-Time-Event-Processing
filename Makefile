.PHONY: up down logs health topics-apply topic-create topic-describe flink-up flink-health flink-job-submit flink-logs flink-api-check flink-anomaly-api-check flink-jdbc-api-check

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

flink-up:
	docker compose up -d --build jobmanager taskmanager
	$(MAKE) flink-health

flink-health:
	@for attempt in $$(seq 1 30); do \
		if curl --fail --silent http://localhost:8081/overview >/dev/null; then \
			echo "PASS: Flink JobManager web UI is reachable at http://localhost:8081"; \
			exit 0; \
		fi; \
		sleep 2; \
	done; \
	echo "FAIL: Flink JobManager web UI did not become reachable within 60 seconds" >&2; \
	exit 1

flink-job-submit:
	docker compose exec -T jobmanager flink run -py /opt/flink/usrlib/jobs/validation_dedup_job.py

flink-logs:
	docker compose logs -f taskmanager

flink-api-check:
	docker compose exec -T jobmanager python3 -c "from pyflink.common import Duration, WatermarkStrategy; from pyflink.common.watermark_strategy import TimestampAssigner; from pyflink.datastream.data_stream import WindowedStream; from pyflink.datastream.window import TumblingEventTimeWindows; help(WatermarkStrategy.for_bounded_out_of_orderness); help(WatermarkStrategy.with_timestamp_assigner); help(WatermarkStrategy.with_idleness); help(TumblingEventTimeWindows.of); help(WindowedStream.allowed_lateness); help(WindowedStream.side_output_late_data); help(WindowedStream.aggregate); help(TimestampAssigner)"

flink-anomaly-api-check:
	docker compose exec -T jobmanager python3 -c "from pyflink.common import Types; from pyflink.datastream.functions import KeyedProcessFunction; from pyflink.datastream.state import ValueStateDescriptor; help(KeyedProcessFunction); help(ValueStateDescriptor); help(Types.LIST); help(Types.LONG)"

flink-jdbc-api-check:
	docker compose exec -T jobmanager python3 -c "from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions; help(JdbcSink.sink); help(JdbcConnectionOptions.JdbcConnectionOptionsBuilder); help(JdbcExecutionOptions.builder)"
