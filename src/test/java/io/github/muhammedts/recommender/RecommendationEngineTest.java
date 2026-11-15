package io.github.muhammedts.recommender;

import static io.github.muhammedts.recommender.RecommendationEngine.ITEM_COL;
import static io.github.muhammedts.recommender.RecommendationEngine.RANK_COL;
import static io.github.muhammedts.recommender.RecommendationEngine.RATING_COL;
import static io.github.muhammedts.recommender.RecommendationEngine.SCORE_COL;
import static io.github.muhammedts.recommender.RecommendationEngine.USER_COL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class RecommendationEngineTest {

    private static final int USERS = 30;
    private static final int BOOKS = 12;
    private static final int TOP_N = 3;

    private static SparkSession spark;
    private static Dataset<Row> ratings;
    private static final Map<Integer, Set<Integer>> RATED = new HashMap<>();

    private final RecommendationEngine engine =
            new RecommendationEngine(JobConfig.fromEnv(Map.of("TOP_N", String.valueOf(TOP_N), "ALS_RANK", "4")));

    @BeforeAll
    static void startSpark() {
        spark = SparkSession.builder()
                .appName("recommendation-engine-test")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        ratings = RecommendationEngine.normalize(sampleRatings()).cache();
    }

    @AfterAll
    static void stopSpark() {
        spark.stop();
    }

    @Test
    void recommendsTopNUnseenBooksPerUserInRankOrder() {
        List<Row> rows = engine.recommend(ratings).collectAsList();
        assertFalse(rows.isEmpty());

        Map<Integer, List<Row>> byUser = new HashMap<>();
        for (Row row : rows) {
            byUser.computeIfAbsent(row.getInt(row.fieldIndex(USER_COL)), u -> new ArrayList<>()).add(row);
        }
        assertEquals(USERS, byUser.size(), "every user should get recommendations");

        for (Map.Entry<Integer, List<Row>> entry : byUser.entrySet()) {
            int user = entry.getKey();
            List<Row> recs = entry.getValue();
            recs.sort((a, b) -> Integer.compare(a.getInt(a.fieldIndex(RANK_COL)), b.getInt(b.fieldIndex(RANK_COL))));

            int unseenBooks = BOOKS - RATED.get(user).size();
            assertEquals(Math.min(TOP_N, unseenBooks), recs.size(), "user " + user);

            double previousScore = Double.POSITIVE_INFINITY;
            for (int i = 0; i < recs.size(); i++) {
                Row rec = recs.get(i);
                assertEquals(i + 1, rec.getInt(rec.fieldIndex(RANK_COL)), "ranks should be 1..N");
                int book = rec.getInt(rec.fieldIndex(ITEM_COL));
                assertFalse(RATED.get(user).contains(book), "user " + user + " already rated book " + book);
                double score = rec.getDouble(rec.fieldIndex(SCORE_COL));
                assertTrue(score <= previousScore, "scores should not increase with rank");
                previousScore = score;
            }
        }
    }

    @Test
    void evaluatesOnAHeldOutSplit() {
        double rmse = engine.evaluate(ratings);

        assertFalse(Double.isNaN(rmse), "the sample is big enough for a held-out split");
        assertTrue(rmse >= 0 && rmse < 5, "RMSE should be within the 1-5 rating scale, got " + rmse);
    }

    @Test
    void normalizeDropsIncompleteRows() {
        StructType schema = new StructType()
                .add(USER_COL, DataTypes.LongType)
                .add(ITEM_COL, DataTypes.LongType)
                .add(RATING_COL, DataTypes.StringType);
        Dataset<Row> raw = spark.createDataFrame(List.of(
                RowFactory.create(1L, 2L, "4"),
                RowFactory.create(1L, null, "5"),
                RowFactory.create(2L, 3L, "not a number")), schema);

        assertEquals(1, RecommendationEngine.normalize(raw).count());
    }

    /**
     * Two taste groups: even users like books 1-6, odd users like books 7-12.
     * Each user rates a different subset, so there is always something left to recommend.
     */
    private static Dataset<Row> sampleRatings() {
        List<Row> rows = new ArrayList<>();
        for (int user = 1; user <= USERS; user++) {
            boolean likesFirstHalf = user % 2 == 0;
            Set<Integer> rated = new HashSet<>();
            for (int book = 1; book <= BOOKS; book++) {
                if ((user + book) % 3 == 0) {
                    continue; // leave gaps so each user has unseen books
                }
                boolean firstHalf = book <= BOOKS / 2;
                int rating = firstHalf == likesFirstHalf ? 5 : 1;
                rows.add(RowFactory.create(user, book, rating));
                rated.add(book);
            }
            RATED.put(user, rated);
        }
        StructType schema = new StructType()
                .add(USER_COL, DataTypes.IntegerType)
                .add(ITEM_COL, DataTypes.IntegerType)
                .add(RATING_COL, DataTypes.IntegerType);
        return spark.createDataFrame(rows, schema);
    }
}
