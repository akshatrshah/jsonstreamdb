package com.jsonstreamdb.consume;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsonstreamdb.model.DocumentEvent;
import com.jsonstreamdb.store.CachingDocumentStore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Consumes {@link DocumentEvent}s from Kafka and applies them to the
 * {@link CachingDocumentStore}.
 *
 * <p>This class is the "asynchronous consumer" half of the architecture: it
 * runs on a dedicated thread, completely independent of the HTTP request
 * thread that produced the event. Throughput on the ingestion side is no
 * longer bounded by how fast documents can be written to disk -- Kafka
 * absorbs the burst, and this consumer drains it at a sustainable rate.</p>
 *
 * <h2>Concurrency model</h2>
 * <p>On every {@code poll()}, the returned records are grouped by partition
 * and each partition's batch is submitted to a fixed-size
 * {@link ExecutorService} (the "worker pool"). Records within a single
 * partition are processed sequentially and in order by one worker; batches
 * from <i>different</i> partitions run in parallel. Because events are
 * produced keyed by {@code documentId} (see {@code DocumentEventProducer}),
 * every event for a given document always lands on the same partition, so
 * per-document ordering (create -&gt; update -&gt; delete) is preserved even
 * though the consumer is multi-threaded.</p>
 *
 * <h2>No-data-loss guarantee</h2>
 * <p>{@code enable.auto.commit} is disabled. Offsets for a poll batch are
 * committed with {@code commitSync} only <i>after</i> every worker has
 * finished applying its records to the store. If the process crashes
 * mid-batch, none of that batch's offsets were committed, so on restart the
 * same records are re-delivered and re-applied. Writes to the store are
 * idempotent (an UPDATE for the same document simply overwrites the file),
 * so this re-delivery is safe -- the system is "at-least-once" with
 * effectively-once outcomes for document state.</p>
 */
public final class DocumentPersistenceConsumer implements Runnable, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DocumentPersistenceConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final ExecutorService workerPool;
    private final CachingDocumentStore store;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    public DocumentPersistenceConsumer(String bootstrapServers, String topic, String groupId,
                                        int workerThreads, CachingDocumentStore store) {
        this.store = store;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Manual offset management - see "No-data-loss guarantee" above.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // New consumer groups (e.g. first run, or after a topic reset) start
        // from the beginning of the log so no historical events are skipped.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Larger batches => fewer poll round-trips and better amortized
        // worker-pool utilization under sustained load.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(List.of(topic));
        this.workerPool = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r, "jsonstreamdb-worker");
            t.setDaemon(true);
            return t;
        });

        log.info("Persistence consumer started: topic={}, group={}, workerThreads={}", topic, groupId, workerThreads);
    }

    @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    continue;
                }
                processBatch(records);
            }
        } catch (WakeupException e) {
            if (running) {
                throw e;
            }
        } finally {
            consumer.close();
            log.info("Persistence consumer stopped");
        }
    }

    /**
     * Fans a poll batch out across the worker pool, one task per partition,
     * waits for every task to finish, and then commits offsets for the whole
     * batch in one call.
     */
    private void processBatch(ConsumerRecords<String, String> records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> byPartition = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), k -> new ArrayList<>())
                    .add(record);
        }

        List<Future<?>> futures = new ArrayList<>(byPartition.size());
        for (List<ConsumerRecord<String, String>> partitionRecords : byPartition.values()) {
            futures.add(workerPool.submit(() -> applyRecords(partitionRecords)));
        }

        // Block until every partition's batch has been applied to the store.
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                // A failed write should not silently advance the offset.
                // Propagate so this poll loop iteration aborts without
                // committing -- the batch will be retried on the next poll.
                throw new RuntimeException("Worker failed to apply event batch", e);
            }
        }

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<String, String>>> entry : byPartition.entrySet()) {
            long lastOffset = entry.getValue().get(entry.getValue().size() - 1).offset();
            offsets.put(entry.getKey(), new OffsetAndMetadata(lastOffset + 1));
        }
        consumer.commitSync(offsets);
    }

    private void applyRecords(List<ConsumerRecord<String, String>> records) {
        for (ConsumerRecord<String, String> record : records) {
            try {
                DocumentEvent event = mapper.readValue(record.value(), DocumentEvent.class);
                store.applyEvent(event);
            } catch (Exception e) {
                log.error("Failed to apply event at offset {} on partition {}: {}",
                        record.offset(), record.partition(), e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    /** Signals the poll loop to stop and shuts down the worker pool. */
    public void shutdown() {
        running = false;
        consumer.wakeup();
        workerPool.shutdown();
    }

    @Override
    public void close() {
        shutdown();
    }
}
