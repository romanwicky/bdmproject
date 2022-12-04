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
public final class Aggregation4 {
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
        JavaMongoRDD<Document> rddCMT = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"vendor_id\": \"CMT\"} }")));
        // Choose sample size
        int sample_size = 50;
        List<Document> data = rddCMT.take(sample_size);
        for(int i = 0; i <= 5;i++){
            System.out.println(data.get(i).toString());
        }
        JavaMongoRDD<Document> rddVTS = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"vendor_id\": \"VTS\"} }")));
        // Choose sample size
        List<Document> data2 = rddVTS.take(sample_size);
        for(int i = 0; i <= 5;i++){
            System.out.println(data2.get(i).toString());
        }

        // Combine two lists - WILL NEED TO SHUFFLE DATA
        List<Document> merged_data = new ArrayList<>();
        merged_data.addAll(data);
        merged_data.addAll(data2);
        Collections.shuffle(merged_data);
        for(Document d: merged_data){
            System.out.println(d.toString());
        }

        JavaRDD<Document> mergedrdd = jsc.parallelize(merged_data);


        // Write to mongodb as collection "TRAINDATAVENDORID"
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "TRAINVENDORID");
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