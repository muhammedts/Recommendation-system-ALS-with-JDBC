package io.github.muhammedts.recommender;

import java.util.Map;
import java.util.Objects;

/**
 * Job settings, read from environment variables so nothing secret lives in code.
 *
 * <p>Every value has a default that matches the companion Laravel app running locally,
 * so the job runs with no setup beyond a reachable database.
 */
public record JobConfig(
        String jdbcUrl,
        String dbUser,
        String dbPassword,
        String ratingsTable,
        String recommendationsTable,
        int topN,
        int rank,
        int maxIter,
        double regParam,
        double testFraction,
        long seed,
        String sparkMaster,
        long intervalMinutes) {

    public JobConfig {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        requirePositive("TOP_N", topN);
        requirePositive("ALS_RANK", rank);
        requirePositive("ALS_MAX_ITER", maxIter);
        if (regParam < 0) {
            throw new IllegalArgumentException("ALS_REG_PARAM must be >= 0, got " + regParam);
        }
        if (testFraction <= 0 || testFraction >= 1) {
            throw new IllegalArgumentException("TEST_FRACTION must be between 0 and 1, got " + testFraction);
        }
        if (intervalMinutes < 0) {
            throw new IllegalArgumentException("INTERVAL_MINUTES must be >= 0, got " + intervalMinutes);
        }
    }

    /** Reads the configuration from the process environment. */
    public static JobConfig fromEnv() {
        return fromEnv(System.getenv());
    }

    static JobConfig fromEnv(Map<String, String> env) {
        Reader r = new Reader(env);
        return new JobConfig(
                r.string("DB_URL", "jdbc:mysql://localhost:3306/books"),
                r.string("DB_USER", "root"),
                r.string("DB_PASSWORD", ""),
                r.string("RATINGS_TABLE", "ratings"),
                r.string("RECOMMENDATIONS_TABLE", "recommendations"),
                r.integer("TOP_N", 10),
                r.integer("ALS_RANK", 10),
                r.integer("ALS_MAX_ITER", 10),
                r.decimal("ALS_REG_PARAM", 0.1),
                r.decimal("TEST_FRACTION", 0.2),
                r.integer("SEED", 42),
                r.string("SPARK_MASTER", "local[*]"),
                r.integer("INTERVAL_MINUTES", 0));
    }

    /** True when the job should keep running and retrain on a fixed delay. */
    public boolean isScheduled() {
        return intervalMinutes > 0;
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0, got " + value);
        }
    }

    private record Reader(Map<String, String> env) {

        String string(String name, String fallback) {
            String value = env.get(name);
            return value == null || value.isBlank() ? fallback : value.trim();
        }

        int integer(String name, int fallback) {
            String value = string(name, null);
            if (value == null) {
                return fallback;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be a whole number, got '" + value + "'", e);
            }
        }

        double decimal(String name, double fallback) {
            String value = string(name, null);
            if (value == null) {
                return fallback;
            }
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be a number, got '" + value + "'", e);
            }
        }
    }
}
