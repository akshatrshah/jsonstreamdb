package com.jsonstreamdb.bench;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Standalone load-testing tool used to reproduce the two headline numbers
 * from the project README:
 *
 * <ol>
 *   <li><b>Ingestion throughput</b>: publishes {@code numDocuments} JSON
 *       documents concurrently and reports how long the persistence
 *       consumer takes to apply all of them.</li>
 *   <li><b>Cache effectiveness</b>: re-reads a sample of documents twice --
 *       once with {@code ?nocache=1} (forcing a disk read) and once normally
 *       (served from the {@link com.jsonstreamdb.store.CachingDocumentStore}
 *       in-memory cache) -- and reports the latency improvement.</li>
 * </ol>
 *
 * <p>Run against a live instance of the application:</p>
 * <pre>
 *   java -cp target/jsonstreamdb.jar com.jsonstreamdb.bench.LoadTest \
 *        http://localhost:8080 10000 64
 * </pre>
 *
 * <p>Arguments: {@code baseUrl} (default {@code http://localhost:8080}),
 * {@code numDocuments} (default {@code 10000}), {@code concurrency}
 * (default {@code 64}).</p>
 *
 * <p>Concurrency is implemented with a plain fixed-size
 * {@link ExecutorService} (one thread per "concurrent request"), not
 * {@link java.util.concurrent.CompletableFuture}'s default common pool --
 * the common pool's parallelism is capped at roughly
 * {@code availableProcessors() - 1}, which silently throttles "concurrency"
 * far below the requested value for I/O-bound work like this.</p>
 */
public final class LoadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    public static void main(String[] args) throws Exception {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080";
        int numDocuments = args.length > 1 ? Integer.parseInt(args[1]) : 10_000;
        int concurrency = args.length > 2 ? Integer.parseInt(args[2]) : 64;

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();

        System.out.printf("=== JSONStreamDB Load Test ===%n");
        System.out.printf("Target: %s | documents: %d | concurrency: %d%n%n", baseUrl, numDocuments, concurrency);

        runIngestionPhase(client, baseUrl, numDocuments, concurrency);

        System.out.println("\nWaiting for the persistence consumer to catch up (10s)...");
        Thread.sleep(10_000);

        runReadBenchmark(client, baseUrl, numDocuments);
    }

    /** Phase 1: concurrently PUTs {@code numDocuments} synthetic JSON documents. */
    private static void runIngestionPhase(HttpClient client, String baseUrl, int numDocuments, int concurrency) throws Exception {
        long start = System.nanoTime();

        ExecutorService executor = Executors.newFixedThreadPool(concurrency);
        List<Future<?>> futures = new ArrayList<>(numDocuments);

        for (int i = 0; i < numDocuments; i++) {
            final int docIndex = i;
            futures.add(executor.submit(() -> {
                Map<String, Object> body = syntheticDocument(docIndex);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(baseUrl + "/documents/doc-" + docIndex))
                        .timeout(REQUEST_TIMEOUT)
                        .PUT(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body)))
                        .build();
                client.send(request, HttpResponse.BodyHandlers.discarding());
                return null;
            }));
        }

        int completed = 0;
        int failed = 0;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                failed++;
                if (failed <= 5) {
                    System.out.println("  request failed: " + e.getCause());
                }
            }
            completed++;
            if (completed % 1000 == 0 || completed == numDocuments) {
                System.out.printf("  ingested %d / %d (failed so far: %d)%n", completed, numDocuments, failed);
            }
        }

        executor.shutdown();

        double seconds = (System.nanoTime() - start) / 1_000_000_000.0;
        System.out.printf("Ingestion request phase complete: %d documents, %d failed, in %.2fs (%.0f req/s)%n",
                numDocuments, failed, seconds, numDocuments / seconds);
    }

    /** Phase 2: compares disk-read latency (cache bypassed) vs. cache-hit latency. */
    private static void runReadBenchmark(HttpClient client, String baseUrl, int numDocuments) throws Exception {
        int sampleSize = Math.min(1000, numDocuments);
        List<Integer> sample = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            sample.add(ThreadLocalRandom.current().nextInt(numDocuments));
        }

        double diskAvgMs = timeReads(client, baseUrl, sample, true);
        double cacheAvgMs = timeReads(client, baseUrl, sample, false);

        double improvementPct = 100.0 * (diskAvgMs - cacheAvgMs) / diskAvgMs;

        System.out.println("\n=== Read Benchmark (sample size = " + sampleSize + ") ===");
        System.out.printf("Average disk read latency  (?nocache=1): %.3f ms%n", diskAvgMs);
        System.out.printf("Average cache hit latency  (default):    %.3f ms%n", cacheAvgMs);
        System.out.printf("Cache speedup:                            %.1f%%%n", improvementPct);
        System.out.println("\nNote: absolute numbers depend on disk type, OS page cache state, and");
        System.out.println("machine load. Run this benchmark on the target machine to get figures");
        System.out.println("representative of that environment (see docs/DESIGN.md).");
    }

    private static double timeReads(HttpClient client, String baseUrl, List<Integer> sample, boolean bypassCache) throws Exception {
        String suffix = bypassCache ? "?nocache=1" : "";
        long totalNanos = 0;
        for (int docIndex : sample) {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/documents/doc-" + docIndex + suffix))
                    .timeout(REQUEST_TIMEOUT)
                    .GET()
                    .build();
            long start = System.nanoTime();
            client.send(request, HttpResponse.BodyHandlers.discarding());
            totalNanos += (System.nanoTime() - start);
        }
        return (totalNanos / 1_000_000.0) / sample.size();
    }

    private static Map<String, Object> syntheticDocument(int index) {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", "item-" + index);
        doc.put("category", "category-" + (index % 20));
        doc.put("value", ThreadLocalRandom.current().nextDouble(0, 1000));
        doc.put("tags", List.of("tag" + (index % 5), "tag" + (index % 7)));
        doc.put("active", index % 3 != 0);
        return doc;
    }
}