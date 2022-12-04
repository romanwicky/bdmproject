package setup;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
public final class Aggregation {
    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // -------------------- Start RDD -------------------
        // Load and analyze data from MongoDB
        long startTime = System.nanoTime();
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaMongoRDD<Document> aggRDDtrip_distance = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"trip_distance\": { $gt: \"2.0\"} } }")));


        // Analyze
        System.out.println(aggRDDtrip_distance.count());
        System.out.println(aggRDDtrip_distance.take(20));
        aggRDDtrip_distance.takeSample(false, 5);

        // -------------------- End RDD -------------------
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");
        jsc.close();
    }
}