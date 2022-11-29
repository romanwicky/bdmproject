package src;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
public final class Main {
    public static void main(final String[] args) throws InterruptedException {
        /* Create the SparkSession.
         * If config arguments are passed from the command line using --conf,
         * parse args for the values to set.
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // More application logic would go here...
        jsc.close();
    }
}