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
public final class Aggregation3 {
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
        JavaMongoRDD<Document> rddrc1 = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"rate_code\": 1} }")));
        // Analyze
        int sample_size = 50;
        List<Document> data = rddrc1.take(sample_size);
        for(int i = 0; i <= 5;i++){
            System.out.println(data.get(i).toString());
        }

        JavaMongoRDD<Document> rddrc2 = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"rate_code\": 2} }")));

        List<Document> data2 = rddrc2.take(sample_size);
        for(int i = 0; i <= 5;i++){
            System.out.println(data2.get(i).toString());
        }

        JavaMongoRDD<Document> rddrc3 = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"rate_code\": 3} }")));

        List<Document> data3 = rddrc3.take(sample_size);
        for(int i = 0; i <= 5;i++){
            System.out.println(data3.get(i).toString());
        }

        JavaMongoRDD<Document> rddrc4 = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"rate_code\": 4} }")));

        List<Document> data4 = rddrc4.take(sample_size);
        for(int i = 0; i <= 5;i++){
            System.out.println(data4.get(i).toString());
        }

        List<Document> merged_data = new ArrayList<>();
        merged_data.addAll(data);
        merged_data.addAll(data2);
        merged_data.addAll(data3);
        merged_data.addAll(data4);
        Collections.shuffle(merged_data);
        for(Document d: merged_data){
            System.out.println(d.toString());
        }

        JavaRDD<Document> mergedrdd = jsc.parallelize(merged_data);

        // Write to mongodb as collection "TRAINRATECODE"
        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "TRAINRATECODE");
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