package com.jsonstreamdb.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.UUID;

/**
 * Wire format for messages published to the {@code jsonstreamdb.document-events}
 * Kafka topic. Each event represents a single mutation (create/update/delete)
 * that a {@code DocumentPersistenceConsumer} will apply to the document store.
 *
 * <p>Events are keyed by {@code documentId} when produced (see
 * {@code DocumentEventProducer}), which guarantees that every event for the
 * same document lands on the same partition and is therefore processed
 * in publish order by exactly one consumer thread at a time.</p>
 */
public final class DocumentEvent {

    public enum Operation {
        CREATE, UPDATE, DELETE
    }

    private final String eventId;
    private final String documentId;
    private final Operation operation;
    private final Map<String, Object> payload;
    private final long timestamp;

    @JsonCreator
    public DocumentEvent(
            @JsonProperty("eventId") String eventId,
            @JsonProperty("documentId") String documentId,
            @JsonProperty("operation") Operation operation,
            @JsonProperty("payload") Map<String, Object> payload,
            @JsonProperty("timestamp") long timestamp) {
        this.eventId = eventId;
        this.documentId = documentId;
        this.operation = operation;
        this.payload = payload;
        this.timestamp = timestamp;
    }

    public static DocumentEvent of(String documentId, Operation operation, Map<String, Object> payload) {
        return new DocumentEvent(UUID.randomUUID().toString(), documentId, operation, payload, System.currentTimeMillis());
    }

    public String getEventId() {
        return eventId;
    }

    public String getDocumentId() {
        return documentId;
    }

    public Operation getOperation() {
        return operation;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "DocumentEvent{eventId=%s, documentId=%s, operation=%s, timestamp=%d}"
                .formatted(eventId, documentId, operation, timestamp);
    }
}
