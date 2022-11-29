package setup;

import com.mongodb.spark.rdd.MongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import scala.reflect.ClassManifestFactory$;

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
        //System.out.println(rdd.first().toJson());
        /*Start Example: Use aggregation to filter a RDD***************/
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"trip_distance\": { $gt: \"2.0\"} } }")));





        /*End Example**************************************************/
        // Analyze data from MongoDB
        System.out.println(aggregatedRdd.count());
        System.out.println(aggregatedRdd.take(5));
        jsc.close();
    }
}