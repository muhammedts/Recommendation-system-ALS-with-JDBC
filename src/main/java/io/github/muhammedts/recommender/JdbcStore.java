package io.github.muhammedts.recommender;

import static org.apache.spark.sql.functions.current_timestamp;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/** Reads ratings from, and writes recommendations to, the application database over JDBC. */
public final class JdbcStore {

    private final JobConfig config;

    public JdbcStore(JobConfig config) {
        this.config = config;
    }

    public Dataset<Row> loadRatings(SparkSession spark) {
        String query = String.format("(SELECT %s, %s, %s FROM %s) AS ratings",
                RecommendationEngine.USER_COL,
                RecommendationEngine.ITEM_COL,
                RecommendationEngine.RATING_COL,
                config.ratingsTable());
        return spark.read().jdbc(config.jdbcUrl(), query, connectionProperties());
    }

    /**
     * Replaces the contents of the recommendations table.
     *
     * <p>The table is truncated rather than dropped, so the schema, keys and indexes created by
     * the web app's migrations stay in place.
     */
    public void replaceRecommendations(Dataset<Row> recommendations) {
        recommendations
                .withColumn("generated_at", current_timestamp())
                .write()
                .mode(SaveMode.Overwrite)
                .option("truncate", "true")
                .jdbc(config.jdbcUrl(), config.recommendationsTable(), connectionProperties());
    }

    private Properties connectionProperties() {
        Properties props = new Properties();
        props.setProperty("user", config.dbUser());
        props.setProperty("password", config.dbPassword());
        // Name the driver explicitly: jars added with spark-submit --jars are not always
        // visible to java.sql.DriverManager's automatic discovery.
        if (config.jdbcUrl().startsWith("jdbc:mysql:")) {
            props.setProperty("driver", "com.mysql.cj.jdbc.Driver");
        }
        return props;
    }
}
