package com.jsonstreamdb.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.jsonstreamdb.ingest.DocumentEventProducer;
import com.jsonstreamdb.model.DocumentEvent;
import com.jsonstreamdb.store.CachingDocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Thin HTTP front-end for JSONStreamDB, built on the JDK's built-in
 * {@code com.sun.net.httpserver} (no web framework dependency needed for a
 * handful of routes).
 *
 * <h2>Routes</h2>
 * <pre>
 *   PUT    /documents/{id}            -> publish CREATE/UPDATE event, returns 202 Accepted
 *   DELETE /documents/{id}            -> publish DELETE event, returns 202 Accepted
 *   GET    /documents/{id}            -> read document (cache-first)
 *   GET    /documents/{id}?nocache=1  -> read document, bypassing the cache (benchmarking)
 *   GET    /stats                     -> cache hit/miss counters
 * </pre>
 *
 * <p>Writes are intentionally "fire and forget" from the caller's
 * perspective: a 202 means the event was durably written to Kafka, not that
 * it has been applied to the store yet. A subsequent GET may briefly return
 * 404 or stale data until {@code DocumentPersistenceConsumer} catches up --
 * this is the standard eventual-consistency trade-off for decoupling
 * ingestion from persistence.</p>
 */
public final class HttpApiServer {

    private static final Logger log = LoggerFactory.getLogger(HttpApiServer.class);
    private static final Pattern DOCUMENT_PATH = Pattern.compile("^/documents/([^/]+)$");

    private final HttpServer server;
    private final ObjectMapper mapper = new ObjectMapper();

    public HttpApiServer(int port, DocumentEventProducer producer, CachingDocumentStore store) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newCachedThreadPool());


        server.createContext("/documents/", exchange -> handleDocuments(exchange, producer, store));
        server.createContext("/stats", exchange -> handleStats(exchange, store));
    }

    public void start() {
        server.start();
        log.info("HTTP API listening on port {}", server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    private void handleDocuments(HttpExchange exchange, DocumentEventProducer producer, CachingDocumentStore store) throws IOException {
        Matcher matcher = DOCUMENT_PATH.matcher(exchange.getRequestURI().getPath());
        if (!matcher.matches()) {
            sendJson(exchange, 404, error("Not found"));
            return;
        }
        String documentId = matcher.group(1);

        try {
            switch (exchange.getRequestMethod()) {
                case "PUT" -> handleUpsert(exchange, producer, documentId);
                case "DELETE" -> handleDelete(exchange, producer, documentId);
                case "GET" -> handleGet(exchange, store, documentId);
                default -> sendJson(exchange, 405, error("Method not allowed"));
            }
        } catch (Exception e) {
            log.error("Request failed", e);
            sendJson(exchange, 500, error("Internal error: " + e.getMessage()));
        }
    }

    private void handleUpsert(HttpExchange exchange, DocumentEventProducer producer, String documentId) throws Exception {
        byte[] body = exchange.getRequestBody().readAllBytes();
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = body.length == 0
                ? Map.of()
                : mapper.readValue(body, Map.class);

        DocumentEvent event = DocumentEvent.of(documentId, DocumentEvent.Operation.UPDATE, payload);
        producer.publish(event).get(); // wait for Kafka ack, not for persistence

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("eventId", event.getEventId());
        response.put("documentId", documentId);
        sendJson(exchange, 202, response);
    }

    private void handleDelete(HttpExchange exchange, DocumentEventProducer producer, String documentId) throws Exception {
        DocumentEvent event = DocumentEvent.of(documentId, DocumentEvent.Operation.DELETE, Map.of());
        producer.publish(event).get();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "queued");
        response.put("eventId", event.getEventId());
        response.put("documentId", documentId);
        sendJson(exchange, 202, response);
    }

    private void handleGet(HttpExchange exchange, CachingDocumentStore store, String documentId) throws IOException {
        boolean bypassCache = exchange.getRequestURI().getQuery() != null
                && exchange.getRequestURI().getQuery().contains("nocache");

        Optional<String> document = store.get(documentId, bypassCache);
        if (document.isEmpty()) {
            sendJson(exchange, 404, error("Document not found (it may not have been persisted yet)"));
            return;
        }
        sendRaw(exchange, 200, document.get());
    }

    private void handleStats(HttpExchange exchange, CachingDocumentStore store) throws IOException {
        CachingDocumentStore.CacheStats stats = store.stats();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("cacheSize", stats.size());
        response.put("cacheMaxSize", stats.maxSize());
        response.put("cacheHits", stats.hits());
        response.put("cacheMisses", stats.misses());
        response.put("cacheHitRate", stats.hitRate());
        sendJson(exchange, 200, response);
    }

    private Map<String, Object> error(String message) {
        return Map.of("error", message);
    }

    private void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        sendRaw(exchange, status, mapper.writeValueAsString(body));
    }

    private void sendRaw(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
