/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

public class GrapeRankInput {
    private String rater;
    private String ratee;
    private double rating;
    private double confidence;

    public GrapeRankInput(String rater, String ratee, double rating, double confidence) {
        this.rater = rater;
        this.ratee = ratee;
        this.rating = rating;
        this.confidence = confidence;
    }

    public String getRater() {
        return rater;
    }

    public void setRater(String rater) {
        this.rater = rater;
    }

    public String getRatee() {
        return ratee;
    }

    public void setRatee(String ratee) {
        this.ratee = ratee;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }
}
