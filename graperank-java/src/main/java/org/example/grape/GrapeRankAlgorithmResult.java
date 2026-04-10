/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

import java.util.Map;

public class GrapeRankAlgorithmResult {
    private final Map<String, ScoreCard> scorecards;
    private final int rounds;

    public GrapeRankAlgorithmResult(Map<String, ScoreCard> scorecards, int rounds) {
        this.scorecards = scorecards;
        this.rounds = rounds;
    }

    public Map<String, ScoreCard> getScorecards() {
        return scorecards;
    }

    public int getRounds() {
        return rounds;
    }
}