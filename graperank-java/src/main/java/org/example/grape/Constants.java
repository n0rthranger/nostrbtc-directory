/*
 * Derived from brainstorm_graperank_algorithm by Pretty-Good-Freedom-Tech / NosFabrica / David Strayhorn.
 * Original: https://github.com/NosFabrica/brainstorm_graperank_algorithm
 * Licensed under AGPL-3.0 — see graperank-java/LICENSE.
 */
package org.example.grape;

public class Constants {

    public static final double DEFAULT_RATING_FOR_FOLLOW = envDouble("GRAPERANK_RATING_FOLLOW", 1.0);
    public static final double DEFAULT_RATING_FOR_MUTE = envDouble("GRAPERANK_RATING_MUTE", -0.1);
    public static final double DEFAULT_RATING_FOR_REPORT = envDouble("GRAPERANK_RATING_REPORT", -0.1);

    public static final double DEFAULT_CONFIDENCE_FOR_FOLLOW = envDouble("GRAPERANK_CONFIDENCE_FOLLOW", 0.03);
    public static final double DEFAULT_CONFIDENCE_FOR_FOLLOW_FROM_OBSERVER = envDouble("GRAPERANK_CONFIDENCE_FOLLOW_OBSERVER", 0.5);
    public static final double DEFAULT_CONFIDENCE_FOR_MUTE = envDouble("GRAPERANK_CONFIDENCE_MUTE", 0.5);
    public static final double DEFAULT_CONFIDENCE_FOR_REPORT = envDouble("GRAPERANK_CONFIDENCE_REPORT", 0.5);

    public static final double GLOBAL_ATTENUATION_FACTOR = envDouble("GRAPERANK_ATTENUATION_FACTOR", 0.85);
    public static final double GLOBAL_RIGOR = envDouble("GRAPERANK_RIGOR", 0.5);

    public static final double THRESHOLD_OF_LOOP_BREAK_GIVEN_MINIMUM_DELTA_INFLUENCE = envDouble("GRAPERANK_CONVERGENCE_THRESHOLD", 0.0001);

    public static final double DEFAULT_CUTOFF_OF_VALID_USER = envDouble("GRAPERANK_CUTOFF_VALID_USER", 0.02);

    public static final double DEFAULT_CUTOFF_OF_TRUSTED_REPORTER = envDouble("GRAPERANK_CUTOFF_TRUSTED_REPORTER", 0.1);

    private static double envDouble(String name, double defaultValue) {
        String val = System.getenv(name);
        if (val == null || val.isEmpty()) return defaultValue;
        try {
            return Double.parseDouble(val);
        } catch (NumberFormatException e) {
            System.err.println("WARNING: Invalid double for env var " + name + "='" + val + "', using default " + defaultValue);
            return defaultValue;
        }
    }
}
