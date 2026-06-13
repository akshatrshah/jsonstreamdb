package com.jsonstreamdb.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jsonstreamdb.model.DocumentEvent;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The storage engine for JSONStreamDB.
 *
 * <p><b>Persistence:</b> every document is stored as its own JSON file under
 * {@code dataDir/<2-hex-digit shard>/<documentId>.json}. Sharding by the low
 * byte of {@code documentId.hashCode()} spreads files across 256
 * sub-directories so no single directory accumulates an unwieldy number of
 * entries as the collection grows past tens of thousands of documents.
 * Writes are atomic: content is written to a temporary file and then moved
 * into place with {@code ATOMIC_MOVE}, so a crash mid-write can never leave
 * behind a half-written document.</p>
 *
 * <p><b>Caching:</b> a {@link ConcurrentHashMap} sits in front of the disk.
 * Reads check the cache first; on a hit, no disk I/O happens at all. On a
 * miss, the document is read from disk and the result is cached
 * (read-through). Writes update both disk and cache in the same call
 * (write-through), so the cache never serves stale data for documents that
 * were written through this store. {@link ConcurrentHashMap} was chosen
 * specifically because the consumer worker pool and the HTTP read path
 * access the cache from many threads concurrently -- it gives lock-free
 * reads and fine-grained (bucket-level) locking on writes, which is exactly
 * the access pattern here (read-heavy, scattered-key writes).</p>
 *
 * <p><b>Eviction:</b> capped at {@code maxCacheEntries}. When the cap is
 * exceeded, the entry with the oldest {@code lastAccess} timestamp is
 * removed (an approximate LRU). This is an O(n) scan over the cache, which
 * is intentional -- see {@code docs/DESIGN.md} for the trade-off discussion
 * versus a true O(1) LRU.</p>
 */
public final class CachingDocumentStore {

    private final Path dataDir;
    private final int maxCacheEntries;
    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentHashMap<String, CacheEntry> cache = new ConcurrentHashMap<>();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();

    public CachingDocumentStore(Path dataDir, int maxCacheEntries) {
        this.dataDir = dataDir;
        this.maxCacheEntries = maxCacheEntries;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to create data directory: " + dataDir, e);
        }
    }

    /** Applies a single consumed event to disk + cache. Called from consumer worker threads. */
    public void applyEvent(DocumentEvent event) {
        switch (event.getOperation()) {
            case CREATE, UPDATE -> upsert(event);
            case DELETE -> delete(event.getDocumentId());
        }
    }

    private void upsert(DocumentEvent event) {
        Map<String, Object> stored = new LinkedHashMap<>();
        stored.put("documentId", event.getDocumentId());
        stored.put("payload", event.getPayload());
        stored.put("updatedAt", event.getTimestamp());
        stored.put("version", event.getEventId());

        try {
            String json = mapper.writeValueAsString(stored);
            writeToDisk(event.getDocumentId(), json);
            cache.put(event.getDocumentId(), new CacheEntry(json));
            evictIfNeeded();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to persist document " + event.getDocumentId(), e);
        }
    }

    private void delete(String documentId) {
        cache.remove(documentId);
        try {
            Files.deleteIfExists(pathFor(documentId));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete document " + documentId, e);
        }
    }

    /**
     * Reads a document, preferring the cache.
     *
     * @param bypassCache if true, skips the cache entirely (used by the
     *                     benchmark harness to measure raw disk-read latency
     *                     for comparison against cache-hit latency)
     */
    public Optional<String> get(String documentId, boolean bypassCache) {
        if (!bypassCache) {
            CacheEntry hit = cache.get(documentId);
            if (hit != null) {
                hit.touch();
                cacheHits.incrementAndGet();
                return Optional.of(hit.json);
            }
            cacheMisses.incrementAndGet();
        }

        Optional<String> fromDisk = readFromDisk(documentId);
        if (!bypassCache) {
            fromDisk.ifPresent(json -> {
                cache.put(documentId, new CacheEntry(json));
                evictIfNeeded();
            });
        }
        return fromDisk;
    }

    private Optional<String> readFromDisk(String documentId) {
        Path path = pathFor(documentId);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Files.readString(path));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read document " + documentId, e);
        }
    }

    private void writeToDisk(String documentId, String json) throws IOException {
        Path dir = pathFor(documentId).getParent();
        Files.createDirectories(dir);
        Path target = pathFor(documentId);
        Path tmp = dir.resolve(documentId + ".json." + Thread.currentThread().getId() + ".tmp");
        Files.writeString(tmp, json);
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private Path pathFor(String documentId) {
        String shard = String.format("%02x", Math.floorMod(documentId.hashCode(), 256));
        return dataDir.resolve(shard).resolve(documentId + ".json");
    }

    /** Evicts least-recently-accessed entries until the cache is back at or under capacity. */
    private void evictIfNeeded() {
        while (cache.size() > maxCacheEntries) {
            String oldestKey = null;
            long oldestAccess = Long.MAX_VALUE;
            for (Map.Entry<String, CacheEntry> entry : cache.entrySet()) {
                long lastAccess = entry.getValue().lastAccess.get();
                if (lastAccess < oldestAccess) {
                    oldestAccess = lastAccess;
                    oldestKey = entry.getKey();
                }
            }
            if (oldestKey == null) {
                break; // cache concurrently emptied by another thread
            }
            cache.remove(oldestKey);
        }
    }

    public CacheStats stats() {
        return new CacheStats(cache.size(), maxCacheEntries, cacheHits.get(), cacheMisses.get());
    }

    /** A single cached document, holding its JSON string and last-access time for LRU eviction. */
    private static final class CacheEntry {
        final String json;
        final AtomicLong lastAccess;

        CacheEntry(String json) {
            this.json = json;
            this.lastAccess = new AtomicLong(System.nanoTime());
        }

        void touch() {
            lastAccess.set(System.nanoTime());
        }
    }

    /** Snapshot of cache effectiveness, exposed via {@code GET /stats}. */
    public record CacheStats(int size, int maxSize, long hits, long misses) {
        public double hitRate() {
            long total = hits + misses;
            return total == 0 ? 0.0 : (double) hits / total;
        }
    }
}
