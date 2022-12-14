package sparkRDD;
// company name

import com.mongodb.spark.config.WriteConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static avro.shaded.com.google.common.primitives.Ints.asList;
import static java.util.Collections.singletonList;

public class RegressionDataset {
    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/YellowTaxiCab.YTC")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/YellowTaxiCab.YTC")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        long startTime = System.nanoTime();
        // -------------------- Start RDD -------------------
        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        // Choose sample size
        int sample_size = 400000;
        List<Document> data = rdd.take(sample_size);

        JavaRDD<Document> mergedrdd = jsc.parallelize(data);

        // Write to mongodb as collection "TRAINDATAVENDORID"
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "REGRESSIONDATASET");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);

        MongoSpark.save(mergedrdd, writeConfig);

        // -------------------- End RDD -------------------
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");
        jsc.close();
    }
}

