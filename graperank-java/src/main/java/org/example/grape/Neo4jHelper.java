/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Neo4jHelper implements AutoCloseable {

    private final Driver driver;

    public Neo4jHelper() {
        String uri = System.getenv("NEO4J_URL");
        String username = System.getenv("NEO4J_USERNAME");
        String password = System.getenv("NEO4J_PASSWORD");

        if (uri == null || uri.isEmpty()) {
            throw new IllegalStateException("NEO4J_URL environment variable is required");
        }
        if (username == null || username.isEmpty()) {
            throw new IllegalStateException("NEO4J_USERNAME environment variable is required");
        }
        if (password == null || password.isEmpty()) {
            throw new IllegalStateException("NEO4J_PASSWORD environment variable is required");
        }

        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
    }

    /**
     * Load the entire social graph in a single Neo4j query.
     * Returns a CachedGraph with all users and relationships.
     */
    public CachedGraph loadFullGraph() {
        String query =
                "MATCH (u:NostrUser)-[r:FOLLOWS|MUTES|REPORTS]->(v:NostrUser) " +
                "RETURN u.pubkey AS source, type(r) AS relationship, v.pubkey AS target";

        List<RelationshipInfo> relationships = new ArrayList<>();
        java.util.Set<String> users = new java.util.HashSet<>();

        try (Session session = driver.session()) {
            session.executeRead(tx -> {
                Result result = tx.run(query);
                while (result.hasNext()) {
                    Record record = result.next();
                    String source = record.get("source").asString();
                    String rel = record.get("relationship").asString();
                    String target = record.get("target").asString();
                    relationships.add(new RelationshipInfo(source, rel, target));
                    users.add(source);
                    users.add(target);
                }
                return null;
            });
        }

        return new CachedGraph(users, relationships);
    }

    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
    }

    public static class RelationshipInfo {
        private final String source;
        private final String relationship;
        private final String target;

        public RelationshipInfo(String source, String relationship, String target) {
            this.source = source;
            this.relationship = relationship;
            this.target = target;
        }

        public String getSource() {
            return source;
        }

        public String getRelationship() {
            return relationship;
        }

        public String getTarget() {
            return target;
        }

        @Override
        public String toString() {
            return "RelationshipInfo{" +
                    "source='" + source + '\'' +
                    ", relationship='" + relationship + '\'' +
                    ", target='" + target + '\'' +
                    '}';
        }
    }
}
