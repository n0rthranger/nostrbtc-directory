/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ScoreCard {
    private final String observer;
    private final String observee;
    private String context = "not a bot";
    private int hops;
    private double averageScore = 0;
    private double input = 0;
    private double confidence = 0;
    private double influence = 0;
    private Boolean verified = null;

    @JsonProperty("trusted_followers")
    private long trustedFollowers = 0;

    @JsonProperty("trusted_reporters")
    private long trustedReporters = 0;

    public ScoreCard(String observer, String observee, int hops) {
        this.observer = observer;
        this.observee = observee;
        this.hops = hops;
    }

    public ScoreCard(String observer, String observee, double averageScore, double input, double confidence,
            double influence) {
        this.observer = observer;
        this.observee = observee;
        this.averageScore = averageScore;
        this.input = input;
        this.confidence = confidence;
        this.influence = influence;
        this.hops = 0;
    }

    public String getObserver() {
        return observer;
    }

    public String getObservee() {
        return observee;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public int getHops() {
        return hops;
    }

    public void setHops(int hops) {
        this.hops = hops;
    }


    public double getAverageScore() {
        return averageScore;
    }

    public void setAverageScore(double averageScore) {
        this.averageScore = averageScore;
    }

    public double getInput() {
        return input;
    }

    public void setInput(double input) {
        this.input = input;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public double getInfluence() {
        return influence;
    }

    public void setInfluence(double influence) {
        this.influence = influence;
    }

    public Boolean getVerified() {
        return verified;
    }

    public void setVerified(Boolean verified) {
        this.verified = verified;
    }

    public long getTrustedFollowers() {
        return trustedFollowers;
    }

    public void setTrustedFollowers(long trustedFollowers) {
        this.trustedFollowers = trustedFollowers;
    }

    public long getTrustedReporters() {
        return trustedReporters;
    }

    public void setTrustedReporters(long trustedReporters) {
        this.trustedReporters = trustedReporters;
    }
}
