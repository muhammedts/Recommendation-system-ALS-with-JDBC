package io.github.muhammedts.recommender;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.row_number;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

/**
 * Trains collaborative-filtering models with Spark ALS and turns them into ranked recommendations.
 *
 * <p>Input ratings need three columns: {@code user_id}, {@code book_id} and {@code rating}.
 * Output rows are {@code user_id, book_id, rank, score}, one per recommendation, with
 * {@code rank} running from 1 to {@code topN} and books the user already rated left out.
 */
public final class RecommendationEngine {

    public static final String USER_COL = "user_id";
    public static final String ITEM_COL = "book_id";
    public static final String RATING_COL = "rating";
    public static final String RANK_COL = "rank";
    public static final String SCORE_COL = "score";

    private final JobConfig config;

    public RecommendationEngine(JobConfig config) {
        this.config = config;
    }

    /** Casts the ratings to the types ALS expects and drops incomplete rows. */
    public static Dataset<Row> normalize(Dataset<Row> ratings) {
        return ratings
                .select(
                        col(USER_COL).cast(DataTypes.IntegerType).as(USER_COL),
                        col(ITEM_COL).cast(DataTypes.IntegerType).as(ITEM_COL),
                        col(RATING_COL).cast(DataTypes.FloatType).as(RATING_COL))
                .na()
                .drop();
    }

    /**
     * Fits a model on a training split and returns the RMSE on the held-out split.
     *
     * @return the RMSE, or {@code NaN} when the held-out split has no users or books seen in training
     */
    public double evaluate(Dataset<Row> ratings) {
        double fraction = config.testFraction();
        Dataset<Row>[] splits = ratings.randomSplit(new double[] {1 - fraction, fraction}, config.seed());
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];
        if (training.isEmpty() || test.isEmpty()) {
            return Double.NaN;
        }

        Dataset<Row> predictions = newAls().fit(training).transform(test);
        if (predictions.isEmpty()) {
            return Double.NaN;
        }
        return new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol(RATING_COL)
                .setPredictionCol("prediction")
                .evaluate(predictions);
    }

    /** Fits a model on all ratings and returns each user's top-N books they have not rated yet. */
    public Dataset<Row> recommend(Dataset<Row> ratings) {
        ALSModel model = newAls().fit(ratings);

        // Ask ALS for enough candidates that, after removing books a user already rated,
        // at least topN remain for every user.
        long mostRatedByOneUser = ratings.groupBy(USER_COL).count().agg(max("count")).first().getLong(0);
        long bookCount = ratings.select(ITEM_COL).distinct().count();
        int candidates = (int) Math.min(bookCount, config.topN() + mostRatedByOneUser);

        Dataset<Row> scored = model.recommendForAllUsers(candidates)
                .select(col(USER_COL), explode(col("recommendations")).as("rec"))
                .select(
                        col(USER_COL),
                        col("rec." + ITEM_COL).as(ITEM_COL),
                        col("rec." + RATING_COL).cast(DataTypes.DoubleType).as(SCORE_COL));

        Dataset<Row> alreadyRated = ratings.select(col(USER_COL).as("seen_user"), col(ITEM_COL).as("seen_book"));
        Dataset<Row> unseen = scored.join(
                alreadyRated,
                scored.col(USER_COL).equalTo(alreadyRated.col("seen_user"))
                        .and(scored.col(ITEM_COL).equalTo(alreadyRated.col("seen_book"))),
                "left_anti");

        WindowSpec byScore = Window.partitionBy(USER_COL).orderBy(col(SCORE_COL).desc(), col(ITEM_COL).asc());
        return unseen
                .withColumn(RANK_COL, row_number().over(byScore))
                .filter(col(RANK_COL).leq(config.topN()))
                .select(USER_COL, ITEM_COL, RANK_COL, SCORE_COL);
    }

    ALS newAls() {
        return new ALS()
                .setUserCol(USER_COL)
                .setItemCol(ITEM_COL)
                .setRatingCol(RATING_COL)
                .setRank(config.rank())
                .setMaxIter(config.maxIter())
                .setRegParam(config.regParam())
                .setSeed(config.seed())
                .setNonnegative(true)
                // Users or books that only appear in the test split get NaN predictions; skip them.
                .setColdStartStrategy("drop");
    }
}
