package org.example.grape;

import java.util.*;

/**
 * In-memory representation of the full Neo4j social graph.
 * Loaded once per cycle, reused for all observer computations.
 */
public class CachedGraph {
    public final List<String> allUsers;
    public final Set<String> allUserSet;
    public final List<Neo4jHelper.RelationshipInfo> allRelationships;
    public final Map<String, List<Neo4jHelper.RelationshipInfo>> outgoingRelationshipsBySource;
    public final Map<String, List<String>> followAdjacency;
    public final Map<String, List<String>> followersByUser;
    public final Map<String, List<String>> reportersByUser;
    public final long loadedAtMs;
    public final int nodeCount;
    public final int edgeCount;

    public CachedGraph(Set<String> users, List<Neo4jHelper.RelationshipInfo> relationships) {
        this.loadedAtMs = System.currentTimeMillis();
        this.allUsers = new ArrayList<>(users);
        this.allUserSet = Collections.unmodifiableSet(new HashSet<>(users));
        this.allRelationships = relationships;
        this.nodeCount = users.size();
        this.edgeCount = relationships.size();

        this.followAdjacency = new HashMap<>();
        this.followersByUser = new HashMap<>();
        this.reportersByUser = new HashMap<>();
        this.outgoingRelationshipsBySource = new HashMap<>();

        for (Neo4jHelper.RelationshipInfo rel : relationships) {
            outgoingRelationshipsBySource
                .computeIfAbsent(rel.getSource(), k -> new ArrayList<>())
                .add(rel);

            switch (rel.getRelationship()) {
                case "FOLLOWS":
                    followAdjacency
                        .computeIfAbsent(rel.getSource(), k -> new ArrayList<>())
                        .add(rel.getTarget());
                    followersByUser
                        .computeIfAbsent(rel.getTarget(), k -> new ArrayList<>())
                        .add(rel.getSource());
                    break;
                case "REPORTS":
                    reportersByUser
                        .computeIfAbsent(rel.getTarget(), k -> new ArrayList<>())
                        .add(rel.getSource());
                    break;
            }
        }
    }
}
