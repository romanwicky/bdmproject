package sparkRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

// Which company drove the most miles?
// Which company made the most money?

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
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        // Get all lines with CMT

        // MOST MILES
        JavaRDD<Document> aggRDDVendorIDCMT = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"vendor_id\": \"CMT\"} }")));

        // Sum the trip_distance
        Double sumrddCMT = aggRDDVendorIDCMT.map(f -> Double.parseDouble(f.get("trip_distance").toString())).reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        });

        JavaMongoRDD<Document> aggRDDVendorIDVTS = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"vendor_id\": \"VTS\"} }")));

        // Sum the trip_distance
        Double sumrddVTS = aggRDDVendorIDVTS.map(f -> Double.parseDouble(f.get("trip_distance").toString())).reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        });


        // MOST MONEY
        Double moneyCMT = aggRDDVendorIDCMT.map(f -> Double.parseDouble(f.get("total_amount").toString())).reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        });

        Double moneyVTS = aggRDDVendorIDVTS.map(f -> Double.parseDouble(f.get("total_amount").toString())).reduce(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        });

        System.out.println("Total Miles CMT " + String.format("%.0f", Double.parseDouble(sumrddCMT.toString())));
        System.out.println("Total Miles VTS " + String.format("%.0f", Double.parseDouble(sumrddVTS.toString())));

        System.out.println("Total Money Earned CMT " + String.format("%.0f", Double.parseDouble(moneyCMT.toString())));
        System.out.println("Total Money Earned VTS " + String.format("%.0f", Double.parseDouble(moneyVTS.toString())));


        // -------------------- End RDD -------------------
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");
        jsc.close();
    }
}