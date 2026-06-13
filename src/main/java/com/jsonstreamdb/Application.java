package com.jsonstreamdb;

import com.jsonstreamdb.api.HttpApiServer;
import com.jsonstreamdb.consume.DocumentPersistenceConsumer;
import com.jsonstreamdb.ingest.DocumentEventProducer;
import com.jsonstreamdb.store.CachingDocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Application entry point. Reads configuration from environment variables
 * (falling back to sensible local-development defaults), then wires together
 * the four pieces of the architecture:
 *
 * <pre>
 *   HTTP API  --publish-->  Kafka topic  --poll-->  Persistence consumer
 *      ^                                                     |
 *      |                                                     v
 *      +-------------------- read path ------------- CachingDocumentStore
 * </pre>
 *
 * <h2>Configuration (environment variables)</h2>
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP_SERVERS} (default {@code localhost:9092})</li>
 *   <li>{@code KAFKA_TOPIC} (default {@code jsonstreamdb.document-events})</li>
 *   <li>{@code CONSUMER_GROUP} (default {@code jsonstreamdb-consumers})</li>
 *   <li>{@code WORKER_THREADS} (default: number of CPU cores)</li>
 *   <li>{@code CACHE_SIZE} (default {@code 5000} documents)</li>
 *   <li>{@code DATA_DIR} (default {@code ./data})</li>
 *   <li>{@code HTTP_PORT} (default {@code 8080})</li>
 * </ul>
 */
public final class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);
    private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 5000L;

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "jsonstreamdb.document-events");
        String consumerGroup = env("CONSUMER_GROUP", "jsonstreamdb-consumers");
        int workerThreads = Integer.parseInt(env("WORKER_THREADS", String.valueOf(Runtime.getRuntime().availableProcessors())));
        int cacheSize = Integer.parseInt(env("CACHE_SIZE", "5000"));
        String dataDir = env("DATA_DIR", "./data");
        int httpPort = Integer.parseInt(env("HTTP_PORT", "8080"));

        log.info("Starting JSONStreamDB: bootstrapServers={}, topic={}, group={}, workerThreads={}, cacheSize={}, dataDir={}, httpPort={}",
                bootstrapServers, topic, consumerGroup, workerThreads, cacheSize, dataDir, httpPort);

        CachingDocumentStore store = new CachingDocumentStore(Path.of(dataDir), cacheSize);
        DocumentEventProducer producer = new DocumentEventProducer(bootstrapServers, topic);
        DocumentPersistenceConsumer consumer = new DocumentPersistenceConsumer(
                bootstrapServers, topic, consumerGroup, workerThreads, store);

        Thread consumerThread = new Thread(consumer, "jsonstreamdb-consumer-loop");
        consumerThread.start();

        HttpApiServer httpServer = new HttpApiServer(httpPort, producer, store);
        httpServer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            httpServer.stop();
            consumer.shutdown();
            producer.close();
            try {
                consumerThread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            log.info("Shutdown complete");
        }, "jsonstreamdb-shutdown"));
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
