/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.util.HexFormat;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;

import org.example.grape.CachedGraph;
import org.example.grape.GrapeRankAlgorithm;
import org.example.grape.GrapeRankResult;
import org.example.grape.MessageQueueReturnValue;

public class Main {

    private static final String QUEUE_NAME = "message_queue";
    private static final String JOB_STARTED_QUEUE_NAME = "job_started_queue";
    // Per-job result keys: graperank:result:{privateId} — avoids race condition with shared queue
    private static final String RESULT_KEY_PREFIX = "graperank:result:";

    private static final String REDIS_HOST;
    private static final int REDIS_PORT;
    private static final String REDIS_PASSWORD;
    private static final String QUEUE_HMAC_SECRET;

    static {
        REDIS_HOST = System.getenv("REDIS_HOST");
        if (REDIS_HOST == null || REDIS_HOST.isEmpty()) {
            throw new IllegalStateException("REDIS_HOST environment variable is required");
        }
        String portStr = System.getenv("REDIS_PORT");
        if (portStr == null || portStr.isEmpty()) {
            throw new IllegalStateException("REDIS_PORT environment variable is required");
        }
        try {
            REDIS_PORT = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("REDIS_PORT must be a valid integer, got: '" + portStr + "'");
        }
        String rawPw = System.getenv("REDIS_PASSWORD");
        REDIS_PASSWORD = (rawPw != null) ? rawPw.trim() : null;
        QUEUE_HMAC_SECRET = System.getenv("GRAPERANK_QUEUE_SECRET");
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    // Graph cache — shared across requests, refreshed on batch or when stale
    private static volatile CachedGraph cachedGraph = null;
    private static final long GRAPH_CACHE_TTL_MS;

    static {
        String ttlStr = System.getenv("GRAPERANK_GRAPH_CACHE_TTL_MS");
        long ttl = 5 * 60 * 1000L; // default 5 minutes
        if (ttlStr != null && !ttlStr.isEmpty()) {
            try {
                ttl = Long.parseLong(ttlStr);
            } catch (NumberFormatException e) {
                System.err.println("WARNING: Invalid GRAPERANK_GRAPH_CACHE_TTL_MS='" + ttlStr + "', using default 300000");
            }
        }
        GRAPH_CACHE_TTL_MS = ttl;
    }

    private static final JedisPool jedisPool;

    static {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        if (REDIS_PASSWORD != null && !REDIS_PASSWORD.isEmpty()) {
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT, 2000, REDIS_PASSWORD);
        } else {
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
        }
    }

    private static Jedis getRedis() {
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        while (true) { // reconnect loop
            try (Jedis redis = getRedis()) {
                System.out.println("Connected to Redis. Waiting for messages on '" + QUEUE_NAME + "'...");

                while (true) { // consume loop
                    try {
                        List<String> result = redis.blpop(30, QUEUE_NAME);

                        if (result != null && result.size() == 2) {
                            String message = verifyAndUnwrapMessage(result.get(1));
                            if (message != null) {
                                processMessage(message);
                            }
                        }

                    } catch (JedisConnectionException e) {
                        System.err.println("Redis connection lost, will reconnect: " + e.getMessage());
                        break;
                    } catch (Exception e) {
                        System.err.println("Error processing message:");
                        e.printStackTrace();
                    }
                }

            } catch (Exception e) {
                System.err.println("Failed to connect to Redis, retrying in 2s...");
                e.printStackTrace();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {}
            }
        }
    }

    private static String verifyAndUnwrapMessage(String wrappedMessage) {
        try {
            if (QUEUE_HMAC_SECRET == null || QUEUE_HMAC_SECRET.isEmpty()) {
                System.err.println("GRAPERANK_QUEUE_SECRET is required; rejecting unsigned job");
                return null;
            }
            JsonNode wrapper = mapper.readTree(wrappedMessage);
            JsonNode payloadNode = wrapper.get("payload");
            JsonNode hmacNode = wrapper.get("hmac");
            if (payloadNode == null || payloadNode.isNull()) {
                System.err.println("Rejecting Redis job: missing 'payload' field");
                return null;
            }
            if (hmacNode == null || hmacNode.isNull()) {
                System.err.println("Rejecting Redis job: missing 'hmac' field");
                return null;
            }
            String payload = payloadNode.asText();
            String supplied = hmacNode.asText();
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(QUEUE_HMAC_SECRET.getBytes(), "HmacSHA256"));
            String expected = HexFormat.of().formatHex(mac.doFinal(payload.getBytes()));
            if (!MessageDigest.isEqual(expected.getBytes(), supplied.getBytes())) {
                System.err.println("Rejecting Redis job with invalid HMAC");
                return null;
            }
            return payload;
        } catch (Exception e) {
            System.err.println("Rejecting malformed signed Redis job");
            return null;
        }
    }

    private static void processJobStarted(int privateId) {
        try (Jedis redis = getRedis()) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("id", privateId);
            String json = mapper.writeValueAsString(payload);
            redis.rpush(JOB_STARTED_QUEUE_NAME, json);
        } catch (Exception e) {
            System.err.println("Error setting job as ongoing:");
            e.printStackTrace();
        }
    }

    private static synchronized CachedGraph getOrRefreshGraph() {
        if (cachedGraph != null && (System.currentTimeMillis() - cachedGraph.loadedAtMs) < GRAPH_CACHE_TTL_MS) {
            return cachedGraph;
        }
        long start = System.currentTimeMillis();
        GrapeRankAlgorithm algo = new GrapeRankAlgorithm();
        cachedGraph = algo.loadGraph();
        System.out.println("Graph cache refreshed — " + cachedGraph.nodeCount + " nodes, "
                + cachedGraph.edgeCount + " edges in " + (System.currentTimeMillis() - start) + "ms");
        return cachedGraph;
    }

    private static void processMessage(String message) {
        try {
            JsonNode parsed = mapper.readTree(message);

            // Batch message: {"type": "batch", "observers": [...], "private_id": N}
            if (parsed.has("type") && "batch".equals(parsed.get("type").asText())) {
                processBatchMessage(parsed);
                return;
            }

            // Single observer message (backward compatible)
            int privateId = parsed.get("private_id").asInt();
            String observer = parsed.get("parameters").asText();

            System.out.println("Processing single observer: " + observer.substring(0, Math.min(16, observer.length())) + "...");
            processJobStarted(privateId);

            CachedGraph graph = getOrRefreshGraph();
            GrapeRankAlgorithm algo = new GrapeRankAlgorithm();
            GrapeRankResult result = algo.computeForObserver(graph, observer);

            try (Jedis redis = getRedis()) {
                MessageQueueReturnValue returnValue = new MessageQueueReturnValue(result, privateId);
                String json = mapper.writeValueAsString(returnValue);
                String resultKey = RESULT_KEY_PREFIX + privateId;
                redis.rpush(resultKey, json);
                redis.expire(resultKey, 300); // TTL 5 min
            }

            System.out.println("Single observer done: " + result.getScorecards().size() + " scorecards, "
                    + result.getRounds() + " rounds, " + result.getDurationSeconds() + "s");

        } catch (Exception e) {
            System.err.println("Error processing message:");
            e.printStackTrace();
        }
    }

    private static void processBatchMessage(JsonNode parsed) {
        try {
            int privateId = parsed.get("private_id").asInt();
            JsonNode observersNode = parsed.get("observers");

            List<String> observers = new ArrayList<>();
            for (JsonNode obs : observersNode) {
                observers.add(obs.asText());
            }

            System.out.println("BATCH: " + observers.size() + " observers");
            processJobStarted(privateId);

            // Always load fresh graph for batch (scheduled cycle)
            long start = System.currentTimeMillis();
            GrapeRankAlgorithm algo = new GrapeRankAlgorithm();
            CachedGraph graph = algo.loadGraph();
            synchronized (Main.class) {
                cachedGraph = graph; // update cache for subsequent single requests
            }
            long loadTime = System.currentTimeMillis() - start;
            System.out.println("BATCH: graph loaded — " + graph.nodeCount + " nodes, "
                    + graph.edgeCount + " edges in " + loadTime + "ms");

            // Compute for each observer
            Map<String, GrapeRankResult> results = new HashMap<>();
            for (String observer : observers) {
                long obsStart = System.currentTimeMillis();
                GrapeRankResult result = algo.computeForObserver(graph, observer);
                long elapsed = System.currentTimeMillis() - obsStart;
                System.out.println("BATCH: " + observer.substring(0, Math.min(16, observer.length()))
                        + "... " + elapsed + "ms, " + result.getRounds() + " rounds");
                results.put(observer, result);
            }

            // Build response
            Map<String, Object> response = new HashMap<>();
            response.put("private_id", privateId);
            response.put("type", "batch");
            response.put("results", results);
            response.put("graph_nodes", graph.nodeCount);
            response.put("graph_edges", graph.edgeCount);

            try (Jedis redis = getRedis()) {
                String json = mapper.writeValueAsString(response);
                String resultKey = RESULT_KEY_PREFIX + privateId;
                redis.rpush(resultKey, json);
                redis.expire(resultKey, 300); // TTL 5 min
            }

            long totalTime = System.currentTimeMillis() - start;
            System.out.println("BATCH: complete — " + observers.size() + " observers in " + totalTime + "ms");

        } catch (Exception e) {
            System.err.println("Error processing batch:");
            e.printStackTrace();
        }
    }
}
