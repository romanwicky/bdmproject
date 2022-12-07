package sparkRDD;

import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import java.util.*;
import java.util.concurrent.TimeUnit;

// rate code

import static java.util.Collections.singletonList;
public final class aggratecode {
    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/YellowTaxiCab.test")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/YellowTaxiCab.test")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        long startTime = System.nanoTime();
        // -------------------- Start RDD -------------------
        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaMongoRDD<Document> rddrc1 = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"rate_code\":  {$lt: \"5\" } }}")));

        // Write to mongodb as collection "TRAINRATECODE"
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "TRAINRATECODEBIG");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        MongoSpark.save(rddrc1, writeConfig);


        // -------------------- End RDD -------------------
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");
        jsc.close();
    }
}
