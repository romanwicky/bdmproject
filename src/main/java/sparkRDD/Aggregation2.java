package sparkRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import java.util.List;
import java.util.concurrent.TimeUnit;

// Which company drove the most miles?

import static java.util.Collections.singletonList;
public final class Aggregation2 {
    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        long startTime = System.nanoTime();
        // -------------------- Start RDD -------------------
        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaMongoRDD<Document> aggRDDVendorIDCMT = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"vendor_id\": \"CMT\"} }")));
        List<Document> data2 = aggRDDVendorIDCMT.take(5);

        // TODO: Fix this
        for(Document d: data2){
            System.out.println(d.toString());
        }
        JavaMongoRDD<Document> aggCMTTotalMiles = aggRDDVendorIDCMT.withPipeline(
                singletonList(Document.parse("{ $group: { _id: \"$vendor_id\", totalMiles: { $sum: \"$trip_distance\" } } }"))
        );

        // Analyze
        List<Document> data = aggCMTTotalMiles.take(5);
        for(Document d: data){
            System.out.println(d.toString());
        }

        // -------------------- End RDD -------------------
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");
        jsc.close();
    }
}