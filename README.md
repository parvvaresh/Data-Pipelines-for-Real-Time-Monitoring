# Data Pipelines for Real-Time Monitoring

This repository contains a small, self-contained example platform that demonstrates a real-time data pipeline using Kafka (KRaft mode), Logstash, Elasticsearch and Kibana. A lightweight synthetic producer (the `fake-app`) generates JSON log events and pushes them to Kafka. Logstash consumes those messages, performs basic parsing, and indexes them into Elasticsearch for analysis and visualization in Kibana.

The setup is optimized for local development and testing using Docker Compose.

## Architecture and runtime flow

- Kafka (Bitnami image) runs in KRaft mode (no Zookeeper). It exposes PLAINTEXT listener on port 9092.
- `kafka-init` is a one-time helper container that waits for Kafka to be ready and creates the configured topic(s).
- `fake-app` is a Go-based producer that emits synthetic log events to the configured Kafka topic at a configurable rate.
- `kafka-ui` (Provectus Kafka UI) provides a web interface for inspecting topics and messages.
- Logstash subscribes to the Kafka topic, applies light parsing and field normalisation, and writes events to Elasticsearch.
- Elasticsearch stores the indexed logs; Kibana connects to Elasticsearch to provide a dashboard and discovery UI.

Service interactions (high level):

1. Start Docker Compose. Services start in the order defined in `docker-compose.yml` and controlled by `depends_on` conditions and healthchecks.
2. Kafka accepts messages from producers at `kafka:9092`.
3. `kafka-init` waits until Kafka is healthy and creates the topic defined by `KAFKA_TOPIC`.
4. `fake-app` produces JSON messages to the topic.
5. Logstash consumes the topic and writes to Elasticsearch (index pattern: `logs-YYYY.MM.dd`).
6. Kibana connects to Elasticsearch for visualization; `kafka-ui` connects to Kafka for topic inspection.

## Key files

- `docker-compose.yml` — main orchestration file defining services, environment interpolation, and volumes.
- `.env` — environment variables used by Compose (image tags, ports, topic names, JVM opts, etc.).
- `fake-app/` — small Go program (producer) and its `Dockerfile`.
- `logstash/pipeline/pipeline.conf` — Logstash pipeline: Kafka input, basic filters, Elasticsearch + stdout outputs.
- `logstash/config/logstash.yml` and `logstash/config/pipelines.yml` — Logstash configuration files used by the container.

## Configuration (.env)

The project loads runtime values from `.env` in the repository root. Notable variables:

- `ELK_VERSION` — Elasticsearch / Kibana / Logstash version tag used with `docker.elastic.co` images.
- `KAFKA_IMAGE` — Kafka docker image (Bitnami). Must be set to a tag that exists on Docker Hub (see Troubleshooting).
- `KAFKA_BROKER_ID` — Kafka node id used in KRaft configuration.
- `KAFKA_BROKERS` — bootstrap server address used by clients (default `kafka:9092`).
- `KAFKA_TOPIC` — topic created by `kafka-init` and consumed by Logstash.
- `KAFKA_PARTITIONS` — number of partitions created for the topic.
- JVM options for Elasticsearch and Logstash: `ES_JAVA_XMS`, `ES_JAVA_XMX`, `LS_JAVA_XMS`, `LS_JAVA_XMX`.
- `FAKE_RATE_PER_SEC`, `FAKE_ERROR_RATE` — controls `fake-app` message rate and error frequency.

Keep in mind: some images (notably Bitnami images) require precise tags (for example `3.8.2-debian-11-r0`). Using `latest` or a non-existent tag will produce Docker manifest errors.

## How to run

Prerequisites:

- Docker and Docker Compose (v2) installed and functional.
- At least a few GB free disk and some memory (Elasticsearch is memory hungry).

Start the stack from the repository root:

```bash
docker compose up -d --build
```

Check service status:

```bash
docker compose ps
docker compose logs -f kafka
docker compose logs -f logstash
docker compose logs -f fake-app
```

Verify the topic exists (exec into the Kafka container and use the bundled tooling):

```bash
docker compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Check Elasticsearch health:

```bash
curl -s http://localhost:9200/_cluster/health | jq
```

Logstash pipelines can be checked via its monitoring endpoint:

```bash
curl -s http://localhost:9600/_node/pipelines
```

Open UIs:

- Kafka UI: http://localhost:8080
- Kibana: http://localhost:5601

## Fake producer behavior

The `fake-app` project is a simple Go program that:

- Reads env vars: `KAFKA_BROKERS`, `KAFKA_TOPIC`, `RATE_PER_SEC`, `ERROR_RATE`.
- Builds structured JSON `LogEvent` messages containing timestamp, host, service, level, path, latency and IDs.
- Writes messages synchronously to Kafka using `segmentio/kafka-go`.
- Runs indefinitely, sleeping between messages to meet the configured rate.

This component is intended as a controllable load generator to exercise the pipeline.

## Logstash pipeline logic

- Input: `kafka` consumer, configured using `${KAFKA_BROKERS}` and `${KAFKA_TOPIC}`.
- Filters: simple renaming of `msg` to `message`, parse `ts` into `@timestamp`, lowercase the `level` and ensure `service` is present.
- Output: index events to Elasticsearch with index pattern `logs-YYYY.MM.dd`, and also print to stdout for debugging.

## Troubleshooting

1. Docker manifest / image tag errors (common):

   Error example: "manifest for bitnami/kafka:3.7 not found".

   Cause: the `KAFKA_IMAGE` value in `.env` references a tag that does not exist on Docker Hub. Bitnami images use specific tags (including OS and revision suffixes).

   Resolution:

   - Inspect available tags using Docker Hub or the registry API:

     ```bash
     curl -s 'https://registry.hub.docker.com/v2/repositories/bitnami/kafka/tags?page_size=100' | jq -r '.results[].name' | head -n 40
     ```

   - Or test a tag locally using `docker manifest inspect`:

     ```bash
     docker manifest inspect bitnami/kafka:3.8.2-debian-11-r0
     ```

   - Update `.env` with a valid tag and re-run `docker compose up -d --build`.

2. Elasticsearch memory issues:

   - Reduce `ES_JAVA_XMS`/`ES_JAVA_XMX` in `.env` for low-memory development machines.

3. Ports already in use:

   - Modify the port bindings in `docker-compose.yml` or stop the service using the port.

4. Topic not created:

   - Check `kafka-init` logs. It waits for Kafka to be healthy and then runs `kafka-topics.sh` to create the configured topic. If Kafka failed or is slow to start, `kafka-init` may not run successfully; check `docker compose logs kafka-init`.

## Contract (inputs / outputs / success criteria)

- Inputs: `.env` environment file, Docker installed, network/ports available.
- Outputs: running containers for Kafka, Logstash, Elasticsearch, Kibana, producing and indexed log documents in Elasticsearch.
- Success criteria: `fake-app` writes messages; Logstash consumes and Elasticsearch shows indexed documents; Kibana and Kafka UI accessible.

## Notes and next steps

- The Docker Compose file currently uses environment interpolation from `.env`. Keep image tags explicit (avoid `latest`) to ensure reproducible environments.
- For more robust Kafka testing, consider using an image that provides KRaft-ready tags or run a multi-node Kafka cluster when replicating production scenarios.

If you want, I can also:

- Add a small health check script or Makefile to simplify common commands.
- Add a minimal Kibana dashboard or sample queries for quick verification.

---

End of README.
