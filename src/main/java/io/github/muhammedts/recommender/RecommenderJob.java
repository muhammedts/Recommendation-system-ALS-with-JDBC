package io.github.muhammedts.recommender;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point: load ratings, report model quality, write fresh recommendations.
 *
 * <p>Runs once and exits by default. Set {@code INTERVAL_MINUTES} to keep it running and retrain
 * on a fixed delay; a failed run is logged and the next one still happens.
 */
public final class RecommenderJob {

    private static final Logger LOG = LoggerFactory.getLogger(RecommenderJob.class);

    private final JobConfig config;
    private final SparkSession spark;
    private final JdbcStore store;
    private final RecommendationEngine engine;

    RecommenderJob(JobConfig config, SparkSession spark) {
        this.config = config;
        this.spark = spark;
        this.store = new JdbcStore(config);
        this.engine = new RecommendationEngine(config);
    }

    public static void main(String[] args) throws InterruptedException {
        JobConfig config = JobConfig.fromEnv();
        SparkSession spark = createSession(config);
        RecommenderJob job = new RecommenderJob(config, spark);

        if (!config.isScheduled()) {
            try {
                job.runOnce();
            } finally {
                spark.stop();
            }
            return;
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdownNow();
            spark.stop();
        }));
        LOG.info("Retraining every {} minute(s)", config.intervalMinutes());
        scheduler.scheduleWithFixedDelay(job::runSafely, 0, config.intervalMinutes(), TimeUnit.MINUTES);
        scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    void runOnce() {
        long started = System.nanoTime();
        Dataset<Row> ratings = RecommendationEngine.normalize(store.loadRatings(spark)).cache();
        try {
            long count = ratings.count();
            if (count == 0) {
                LOG.warn("No ratings in '{}' yet; nothing to recommend", config.ratingsTable());
                return;
            }
            LOG.info("Loaded {} ratings", count);

            double rmse = engine.evaluate(ratings);
            LOG.info("Held-out RMSE: {}", Double.isNaN(rmse) ? "n/a (not enough data)" : String.format("%.4f", rmse));

            Dataset<Row> recommendations = engine.recommend(ratings).cache();
            try {
                store.replaceRecommendations(recommendations);
                LOG.info("Wrote {} recommendations to '{}' in {} s",
                        recommendations.count(),
                        config.recommendationsTable(),
                        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started));
            } finally {
                recommendations.unpersist();
            }
        } finally {
            ratings.unpersist();
        }
    }

    private void runSafely() {
        try {
            runOnce();
        } catch (RuntimeException e) {
            LOG.error("Recommendation run failed; will retry on the next interval", e);
        }
    }

    private static SparkSession createSession(JobConfig config) {
        SparkSession.Builder builder = SparkSession.builder().appName("book-recommender-als");
        // spark-submit sets spark.master itself; only fall back to SPARK_MASTER when run directly.
        if (System.getProperty("spark.master") == null) {
            builder.master(config.sparkMaster());
        }
        return builder.getOrCreate();
    }
}
