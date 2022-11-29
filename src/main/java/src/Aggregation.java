package src;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import static java.util.Collections.singletonList;
public final class Aggregation {
    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        System.out.println(rdd.first().toJson());
        /*Start Example: Use aggregation to filter a RDD***************/
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"trip_distance\" : \"1.7\" } }")));
        /*End Example**************************************************/
        // Analyze data from MongoDB
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.first().toJson());
        jsc.close();
    }
}