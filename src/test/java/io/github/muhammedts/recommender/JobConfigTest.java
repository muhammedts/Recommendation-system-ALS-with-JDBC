package io.github.muhammedts.recommender;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JobConfigTest {

    @Test
    void defaultsMatchTheLocalWebApp() {
        JobConfig config = JobConfig.fromEnv(Map.of());

        assertEquals("jdbc:mysql://localhost:3306/books", config.jdbcUrl());
        assertEquals("ratings", config.ratingsTable());
        assertEquals("recommendations", config.recommendationsTable());
        assertEquals(10, config.topN());
        assertFalse(config.isScheduled());
    }

    @Test
    void readsOverridesFromTheEnvironment() {
        JobConfig config = JobConfig.fromEnv(Map.of(
                "DB_URL", "jdbc:mysql://db:3306/app",
                "DB_PASSWORD", "secret",
                "TOP_N", "5",
                "ALS_REG_PARAM", "0.05",
                "INTERVAL_MINUTES", "15"));

        assertEquals("jdbc:mysql://db:3306/app", config.jdbcUrl());
        assertEquals("secret", config.dbPassword());
        assertEquals(5, config.topN());
        assertEquals(0.05, config.regParam());
        assertTrue(config.isScheduled());
    }

    @Test
    void blankValuesFallBackToDefaults() {
        assertEquals(10, JobConfig.fromEnv(Map.of("TOP_N", "  ")).topN());
    }

    @Test
    void rejectsValuesThatAreNotNumbers() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> JobConfig.fromEnv(Map.of("TOP_N", "ten")));

        assertTrue(error.getMessage().contains("TOP_N"));
    }

    @Test
    void rejectsOutOfRangeValues() {
        assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(Map.of("TOP_N", "0")));
        assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(Map.of("TEST_FRACTION", "1")));
        assertThrows(IllegalArgumentException.class, () -> JobConfig.fromEnv(Map.of("INTERVAL_MINUTES", "-1")));
    }
}
