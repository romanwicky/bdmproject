package setup;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import java.io.File;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static avro.shaded.com.google.common.primitives.Ints.asList;

public class UploadDataToMongo {

    public static void main(final String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/bdmfinal.NYT")
                .getOrCreate();
        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        long startTime = System.nanoTime();
        // -------------------- Start RDD -------------------
        String csvfile = "C:/Users/rwick/Downloads/archive/yellow_tripdata_2012-01.csv";
        StructType schema = new StructType().add("vendor_id", "string").
                                            add("pickup_datetime", "string").
                                            add("dropoff_datetime", "string").
                                            add("passenger_count", "long").
                                            add("trip_distance", "long");
        Dataset<Row> df = spark.read().option("mode", "DROPMALFORMED").schema(schema).csv(csvfile);
        JavaRDD<Row> dfrdd = df.toJavaRDD();
        JavaRDD<Document> dfrddoc = dfrdd.map(ft -> Document.parse(ft.toString()));



        Map<String, String> writeOverrides = new HashMap<>();
        writeOverrides.put("collection", "TEST");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeOverrides);
        MongoSpark.save(dfrddoc, writeConfig);





        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");
        jsc.close();
    }
}
