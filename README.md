# JSONStreamDB

A small event-driven document store written in Java that decouples
**write ingestion** from **persistence** using Kafka, and speeds up reads
with an in-memory caching layer built on `ConcurrentHashMap`.

```
            PUT/DELETE /documents/{id}              GET /documents/{id}
                    |                                       |
                    v                                       v
            +---------------+                     +-----------------------+
            |  HTTP API      |                     |  CachingDocumentStore  |
            |  (ingestion)   |                     |  - ConcurrentHashMap   |
            +-------+-------+                     |    read-through cache |
                    |  publish                     |  - JSON files on disk |
                    v                              +-----------+-----------+
            +---------------+                                  ^
            |     Kafka      |                                  |
            |  (event log)   |                                  | applyEvent()
            +-------+-------+                                  |
                    |  poll                                    |
                    v                                          |
            +-----------------------------+                    |
            | DocumentPersistenceConsumer  |--------------------+
            | - poll loop (1 thread)       |
            | - worker pool (N threads),   |
            |   one task per partition     |
            +-----------------------------+
```

## Why this exists

This is a portfolio project demonstrating an event-driven storage
architecture: HTTP writes are translated into events and published to Kafka
immediately (fast, durable, ordered-per-key), while a separate consumer
pool applies those events to the actual store at whatever rate the storage
layer can sustain. Reads are served from a `ConcurrentHashMap` cache in
front of a simple JSON-file-per-document disk layout.


## Architecture at a glance

| Component | File | Responsibility |
|---|---|---|
| `DocumentEvent` | `model/DocumentEvent.java` | Wire format for CREATE/UPDATE/DELETE events |
| `DocumentEventProducer` | `ingest/DocumentEventProducer.java` | Publishes events to Kafka (idempotent, `acks=all`) |
| `DocumentPersistenceConsumer` | `consume/DocumentPersistenceConsumer.java` | Polls Kafka, fans batches out to a worker-thread pool, commits offsets only after successful writes |
| `CachingDocumentStore` | `store/CachingDocumentStore.java` | `ConcurrentHashMap` read-through/write-through cache over JSON files on disk |
| `HttpApiServer` | `api/HttpApiServer.java` | Minimal REST API (built on `com.sun.net.httpserver`, no framework) |
| `Application` | `Application.java` | Wires everything together, handles config + graceful shutdown |
| `LoadTest` | `bench/LoadTest.java` | Ingests 10k+ documents and benchmarks cache vs. disk read latency |

## Running it locally

### 1. Start Kafka

```bash
docker compose up -d
```

This brings up a single-node Kafka broker (KRaft mode, no Zookeeper),
exposed on the host at `localhost:9094` (mapped to the container's
internal port 9092), with topic auto-creation enabled.

> **Port already in use?** If `9094` (or `9092`) conflicts with something
> else on your machine, change the `ports:` mapping and
> `KAFKA_ADVERTISED_LISTENERS` value in `docker-compose.yml` to a free
> port, then use that port in step 3 below.

### 2. Build

```bash
mvn package
```

Produces `target/jsonstreamdb.jar` (a self-contained fat jar).

### 3. Run the server

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 java -jar target/jsonstreamdb.jar
```

> **Running on a different host/port?** `KAFKA_BOOTSTRAP_SERVERS` is the
> only place Kafka's address is referenced. If Kafka isn't on
> `localhost:9094` (e.g. a different machine, a different port mapping,
> or a shared Kafka cluster), just change this environment variable --
> nothing in the source code is hardcoded.

Other optional environment variables (defaults shown):

```bash
KAFKA_TOPIC=jsonstreamdb.document-events
CONSUMER_GROUP=jsonstreamdb-consumers
WORKER_THREADS=<number of CPU cores>
CACHE_SIZE=5000
DATA_DIR=./data
HTTP_PORT=8080
```

### 4. Use the API

```bash
# Create / update a document (returns as soon as Kafka acks the write)
curl -X PUT http://localhost:8080/documents/user-42 \
     -H 'Content-Type: application/json' \
     -d '{"name": "Ada Lovelace", "role": "Engineer"}'

# Read it back (served from cache after the consumer applies the event)
curl http://localhost:8080/documents/user-42

# Force a disk read, bypassing the cache (used by the benchmark tool)
curl 'http://localhost:8080/documents/user-42?nocache=1'

# Delete it
curl -X DELETE http://localhost:8080/documents/user-42

# Cache statistics
curl http://localhost:8080/stats
```

> **Running the server on a different port?** `8080` is just the default
> for `HTTP_PORT`. If you change it, update the `localhost:8080` in these
> commands to match.

A `PUT`/`DELETE` returns `202 Accepted` immediately -- it confirms the event
was durably written to Kafka, not that it has been persisted to disk yet.
A `GET` immediately afterward may return `404` for a brief moment until the
`DocumentPersistenceConsumer` catches up. This is the deliberate trade-off
of decoupling ingestion from persistence (see `docs/DESIGN.md`).

### 5. Run the benchmark

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 java -cp target/jsonstreamdb.jar com.jsonstreamdb.bench.LoadTest \
     http://localhost:8080 10000 64
```

This will:

1. Concurrently `PUT` 10,000 synthetic JSON documents.
2. Wait for the persistence consumer to catch up.
3. Sample 1,000 documents and compare average read latency with the cache
   bypassed (`?nocache=1`, forced disk read) vs. the default cache-first
   path, printing the percentage improvement.

## Design highlights

- **Decoupled ingestion**: HTTP writes only need Kafka to ack before
  returning, so ingestion throughput is bounded by Kafka, not by disk I/O.
- **Ordering per document**: events are published keyed by `documentId`, so
  every event for a given document lands on the same partition and is
  applied in order, even with a multi-threaded consumer.
- **No data loss**: the consumer disables auto-commit and only commits
  offsets after a poll batch has been fully applied to the store. A crash
  mid-batch results in safe re-delivery (writes are idempotent overwrites).
- **Concurrent worker pool**: each `poll()` batch is grouped by partition
  and processed in parallel by a fixed-size `ExecutorService`, one task per
  partition, so persistence throughput scales with partition count and CPU
  cores.
- **`ConcurrentHashMap` cache**: read-through + write-through cache with
  bounded LRU eviction, sized via `CACHE_SIZE`. Cache hits skip disk
  entirely.
- **Atomic disk writes**: documents are written to a temp file and moved
  into place with `ATOMIC_MOVE`, so a crash mid-write never corrupts a
  document file.
