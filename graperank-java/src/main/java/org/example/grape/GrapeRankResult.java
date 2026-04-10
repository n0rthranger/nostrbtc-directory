/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class GrapeRankResult {
    private Map<String, ScoreCard> scorecards;
    private Integer rounds;
    @JsonProperty("duration_seconds")
    private double durationSeconds;
    private boolean success = false;

    public GrapeRankResult(Map<String, ScoreCard> scorecards, Integer rounds, double durationSeconds, boolean success) {
        this.scorecards = scorecards;
        this.rounds = rounds;
        this.durationSeconds = durationSeconds;
        this.success = success;
    }

    public Map<String, ScoreCard> getScorecards() {
        return scorecards;
    }

    public void setScorecards(Map<String, ScoreCard> scorecards) {
        this.scorecards = scorecards;
    }

    public Integer getRounds() {
        return rounds;
    }

    public void setRounds(Integer rounds) {
        this.rounds = rounds;
    }

    public double getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(double durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
