package com.jsonstreamdb.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsonstreamdb.model.DocumentEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes {@link DocumentEvent}s to Kafka. This is the "ingestion" side of
 * JSONStreamDB: an HTTP request that wants to create/update/delete a document
 * is translated into an event here and handed off to Kafka. The HTTP handler
 * returns as soon as Kafka acknowledges the write to its log -- it does
 * <b>not</b> wait for the document to be persisted to the store. That
 * decoupling is what lets ingestion absorb bursts of traffic independently
 * of how fast the storage layer can apply writes.
 */
public final class DocumentEventProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper mapper = new ObjectMapper();

    public DocumentEventProducer(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Idempotent producer + acks=all => each event is written to the Kafka
        // log exactly once and is durable on all in-sync replicas before the
        // produce call is acknowledged. Combined with manual offset commits on
        // the consumer side (see DocumentPersistenceConsumer), this is the
        // backbone of the "no data loss" guarantee.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // Small batching window: trades a few milliseconds of latency for much
        // higher throughput when many requests arrive concurrently.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        this.producer = new KafkaProducer<>(props);
    }

    /**
     * Serializes and publishes an event, keyed by document ID so that all
     * events for a given document are strictly ordered on a single partition.
     *
     * @return a future that completes once Kafka has acknowledged the write
     */
    public CompletableFuture<RecordMetadata> publish(DocumentEvent event) {
        CompletableFuture<RecordMetadata> future = new CompletableFuture<>();
        try {
            String value = mapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.getDocumentId(), value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(metadata);
                }
            });
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
