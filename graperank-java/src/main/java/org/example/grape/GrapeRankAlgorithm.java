/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

import java.util.Map;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class GrapeRankAlgorithm {

    private static final int MAX_HOPS = 8;
    private static final int MAX_ITERATIONS;

    static {
        String val = System.getenv().getOrDefault("GRAPERANK_MAX_ITERATIONS", "100");
        int parsed;
        try {
            parsed = Integer.parseInt(val);
        } catch (NumberFormatException e) {
            System.err.println("WARNING: Invalid GRAPERANK_MAX_ITERATIONS='" + val + "', using default 100");
            parsed = 100;
        }
        MAX_ITERATIONS = parsed;
    }

    public static GrapeRankAlgorithmResult graperankAlgorithm(
            Map<String, List<GrapeRankInput>> graperankInputs,
            Map<String, ScoreCard> graperankScorecards) {

        int rounds = 0;

        // Pre-index rater → ratees once. Replaces the inner O(|inputs|) scan
        // that previously ran for every changed node, collapsing cost per round
        // from O(|worklist| * |inputs|) to O(|worklist| * avg_fanout).
        Map<String, Set<String>> raterToRatees = new HashMap<>();
        for (Map.Entry<String, List<GrapeRankInput>> entry : graperankInputs.entrySet()) {
            String ratee = entry.getKey();
            for (GrapeRankInput input : entry.getValue()) {
                raterToRatees
                        .computeIfAbsent(input.getRater(), k -> new HashSet<>())
                        .add(ratee);
            }
        }

        // Worklist: set of observee keys whose inputs may have changed
        Set<String> worklist = new HashSet<>(graperankScorecards.keySet());

        while (rounds < MAX_ITERATIONS && !worklist.isEmpty()) {
            Set<String> nextWorklist = new HashSet<>();

            for (String key : worklist) {
                ScoreCard scorecard = graperankScorecards.get(key);
                if (scorecard == null) continue;

                if (scorecard.getObserver().equals(scorecard.getObservee())) {
                    continue;
                }

                List<GrapeRankInput> relevantDataPoints = graperankInputs.getOrDefault(scorecard.getObservee(), List.of());

                double sumOfWeights = 0;
                double sumOfWxr = 0;

                for (GrapeRankInput relevantDataPoint : relevantDataPoints) {
                    ScoreCard raterCard = graperankScorecards.get(relevantDataPoint.getRater());
                    if (raterCard == null) continue;
                    double infOfRater = raterCard.getInfluence();
                    double weight = relevantDataPoint.getConfidence()
                            * infOfRater
                            * Constants.GLOBAL_ATTENUATION_FACTOR;

                    double wxr = weight * relevantDataPoint.getRating();

                    sumOfWeights += weight;
                    sumOfWxr += wxr;
                }

                double avgScore = (sumOfWeights != 0) ? sumOfWxr / sumOfWeights : 0;
                scorecard.setAverageScore(avgScore);
                scorecard.setInput(sumOfWeights);

                scorecard.setConfidence(convertInputToConfidence(scorecard.getInput(), Constants.GLOBAL_RIGOR));

                double computedInfluence = Math.max(scorecard.getAverageScore() * scorecard.getConfidence(), 0);
                double deltaInfluence = Math.abs(computedInfluence - scorecard.getInfluence());

                if (deltaInfluence > Constants.THRESHOLD_OF_LOOP_BREAK_GIVEN_MINIMUM_DELTA_INFLUENCE) {
                    // O(1) lookup: every ratee that depends on this rater
                    Set<String> dependents = raterToRatees.get(key);
                    if (dependents != null) {
                        nextWorklist.addAll(dependents);
                    }
                }

                scorecard.setInfluence(computedInfluence);
            }

            rounds++;
            worklist = nextWorklist;
        }

        for (ScoreCard scorecard : graperankScorecards.values()) {
            scorecard.setVerified(scorecard.getInfluence() >= Constants.DEFAULT_CUTOFF_OF_VALID_USER);
        }

        return new GrapeRankAlgorithmResult(graperankScorecards, rounds);
    }

    public static double convertInputToConfidence(double input, double rigor) {
        double decayRate = -Math.log(rigor);
        double exponentialTerm = Math.exp(-input * decayRate);
        return 1 - exponentialTerm;
    }

    // --- Graph loading (single Neo4j query) ---

    public CachedGraph loadGraph() {
        try (Neo4jHelper neo4jHelper = new Neo4jHelper()) {
            return neo4jHelper.loadFullGraph();
        }
    }

    // --- BFS hop distance computation (in-memory) ---

    private Map<String, Integer> bfs(Map<String, List<String>> adjacency, String start, int maxHops) {
        Map<String, Integer> distances = new HashMap<>();
        distances.put(start, 0);
        Queue<String> queue = new LinkedList<>();
        queue.add(start);

        while (!queue.isEmpty()) {
            String current = queue.poll();
            int d = distances.get(current);
            if (d >= maxHops) continue;

            List<String> neighbors = adjacency.getOrDefault(current, Collections.emptyList());
            for (String neighbor : neighbors) {
                if (!distances.containsKey(neighbor)) {
                    distances.put(neighbor, d + 1);
                    queue.add(neighbor);
                }
            }
        }
        return distances;
    }

    // --- Build GrapeRankInputs from cached relationships (observer-specific) ---

    private Map<String, List<GrapeRankInput>> buildInputsForObserver(
            CachedGraph graph, String observer, Set<String> relevantUserSet) {
        Map<String, List<GrapeRankInput>> inputs = new HashMap<>();

        for (String source : relevantUserSet) {
            List<Neo4jHelper.RelationshipInfo> rels = graph.outgoingRelationshipsBySource.get(source);
            if (rels == null) continue;

            for (Neo4jHelper.RelationshipInfo rel : rels) {
                String type = rel.getRelationship();
                double rating;
                double confidence;

                switch (type) {
                    case "FOLLOWS":
                        rating = Constants.DEFAULT_RATING_FOR_FOLLOW;
                        confidence = rel.getSource().equals(observer)
                                ? Constants.DEFAULT_CONFIDENCE_FOR_FOLLOW_FROM_OBSERVER
                                : Constants.DEFAULT_CONFIDENCE_FOR_FOLLOW;
                        break;
                    case "MUTES":
                        rating = Constants.DEFAULT_RATING_FOR_MUTE;
                        confidence = Constants.DEFAULT_CONFIDENCE_FOR_MUTE;
                        break;
                    case "REPORTS":
                        rating = Constants.DEFAULT_RATING_FOR_REPORT;
                        confidence = Constants.DEFAULT_CONFIDENCE_FOR_REPORT;
                        break;
                    default:
                        continue;
                }

                GrapeRankInput input = new GrapeRankInput(rel.getSource(), rel.getTarget(), rating, confidence);
                inputs.computeIfAbsent(input.getRatee(), k -> new ArrayList<>()).add(input);
            }
        }

        return inputs;
    }

    // --- Scorecard initialization ---

    public Map<String, ScoreCard> initGrapeRankScorecards(
            List<String> relevantUsers, String observer, Map<String, Integer> hopDistances) {
        Map<String, ScoreCard> result = new HashMap<>();

        for (String user : relevantUsers) {
            if (!user.equals(observer)) {
                int distance = hopDistances.getOrDefault(user, 999);
                result.put(user, new ScoreCard(observer, user, distance));
            } else {
                result.put(user, new ScoreCard(
                        observer, user, 1.0, Double.POSITIVE_INFINITY, 1.0, 1.0));
            }
        }

        return result;
    }

    // --- Main computation: one observer against a cached graph ---

    public GrapeRankResult computeForObserver(CachedGraph graph, String observer) {
        long startTime = System.currentTimeMillis();

        // BFS to find reachable users + hop distances (in-memory, no Neo4j)
        Map<String, Integer> hopDistances = bfs(graph.followAdjacency, observer, MAX_HOPS);

        // All reachable users = everything BFS found
        // If observer is isolated (not in graph), return empty
        if (hopDistances.size() <= 1 && !graph.allUserSet.contains(observer)) {
            return new GrapeRankResult(new HashMap<>(), 0, 0, false);
        }

        // Use all users in the graph as relevant (same connected component)
        List<String> relevantUsers = graph.allUsers;
        Set<String> relevantUserSet = graph.allUserSet;

        // Build observer-specific GrapeRankInputs from pre-indexed relationships
        Map<String, List<GrapeRankInput>> graperankInputs =
                buildInputsForObserver(graph, observer, relevantUserSet);

        // Init scorecards
        Map<String, ScoreCard> scorecards = initGrapeRankScorecards(relevantUsers, observer, hopDistances);

        // Run algorithm
        long algoStart = System.currentTimeMillis();
        GrapeRankAlgorithmResult algorithmResult = graperankAlgorithm(graperankInputs, scorecards);
        long algoTime = System.currentTimeMillis() - algoStart;

        Map<String, ScoreCard> finalScorecards = algorithmResult.getScorecards();

        // Count trusted followers/reporters from cached data
        for (Map.Entry<String, ScoreCard> entry : finalScorecards.entrySet()) {
            String userPubkey = entry.getKey();
            ScoreCard scoreCard = entry.getValue();

            List<String> followers = graph.followersByUser.getOrDefault(userPubkey, Collections.emptyList());
            long trustedFollowers = followers.stream()
                    .filter(fpk -> {
                        ScoreCard fsc = finalScorecards.get(fpk);
                        return fsc != null && fsc.getInfluence() > Constants.DEFAULT_CUTOFF_OF_VALID_USER;
                    }).count();
            scoreCard.setTrustedFollowers(trustedFollowers);

            List<String> reporters = graph.reportersByUser.getOrDefault(userPubkey, Collections.emptyList());
            long trustedReporters = reporters.stream()
                    .filter(rpk -> {
                        ScoreCard rsc = finalScorecards.get(rpk);
                        return rsc != null && rsc.getInfluence() > Constants.DEFAULT_CUTOFF_OF_TRUSTED_REPORTER;
                    }).count();
            scoreCard.setTrustedReporters(trustedReporters);
        }

        long totalTime = System.currentTimeMillis() - startTime;
        return new GrapeRankResult(
                finalScorecards,
                algorithmResult.getRounds(),
                totalTime / 1000.0,
                true);
    }

    // --- Legacy single-observer entry point (loads fresh graph each time) ---

    public GrapeRankResult graperankAllSteps(String observer) {
        long startTime = System.currentTimeMillis();
        CachedGraph graph = loadGraph();
        System.out.println("PROFILE: graph loaded — " + graph.nodeCount + " nodes, " + graph.edgeCount + " edges in "
                + (System.currentTimeMillis() - startTime) + "ms");
        return computeForObserver(graph, observer);
    }
}
