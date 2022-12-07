package sparkML;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassificationSummary;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import static java.util.Collections.min;

public final class RandForestCompanyName {

    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/YellowTaxiCab.TRAINVENDORID")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/YellowTaxiCab.TRAINVENDORID")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // Load and analyze data from MongoDB
        long startTime = System.nanoTime();
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        List<Document> sample = rdd.takeSample(false, 160000, 0);
        //sample as big as memory put thru network, delete sample from memory .clear, grab another sample
        //repeat (see if you can .fit multiple times)...get multiple rdd.take samples and see how much you can get in row data
        //try different sample sizes and see if there are differeces in accuracy
        //any other experimentation
        List<Row> rowdata = new ArrayList<>();
        for (Document item : sample) {
            String company = (String) item.get("vendor_id");
            double VTS = 0;
            if (company.equals( "VTS")){
                VTS = 1;
            }
            double pickup_hour = Double.parseDouble(item.get("pickup_datetime").toString().substring(11, 13));
            double pickup_minutes = Double.parseDouble(item.get("pickup_datetime").toString().substring(14, 16));
            double dropoff_hour = Double.parseDouble(item.get("dropoff_datetime").toString().substring(11, 13));
            double dropoff_minutes = Double.parseDouble(item.get("dropoff_datetime").toString().substring(14, 16));
            double length = 0;
            if (pickup_hour == dropoff_hour) {
                length = dropoff_minutes - pickup_minutes;
            } else {
                if (pickup_hour < dropoff_hour) {
                    double hour_diff = dropoff_hour - pickup_hour;
                    double minute_diff = dropoff_minutes - pickup_minutes;
                    length = hour_diff * 60 + minute_diff;
                }
                if (pickup_hour > dropoff_hour) {
                    double hours_to_midnight = 24 - pickup_hour;
                    double hours_from_midnight = dropoff_hour;
                    double hour_diff = hours_to_midnight + hours_from_midnight;
                    double minute_diff = dropoff_minutes - pickup_minutes;
                    length = hour_diff * 60 + minute_diff;
                }
            }
            double tolls_amount = Double.parseDouble(item.get("tolls_amount").toString());
            double rate_code = Double.parseDouble(item.get("rate_code").toString());
            double r1 = 0;
            double r2 = 0;
            double r3 = 0;
            double r4 = 0;
            if (rate_code == 1){
                r1 = 1;
            }
            if (rate_code == 2){
                r2 = 1;
            }
            if (rate_code == 3){
                r3 = 1;
            }
            if (rate_code == 4){
                r4 = 1;
            }

            double tip_amount = Double.parseDouble(item.get("tip_amount").toString());
            double trip_distance = Double.parseDouble(item.get("trip_distance").toString());

            rowdata.add(RowFactory.create(VTS, Vectors.dense(trip_distance,
                    tolls_amount, length, tip_amount, r1, r2, r3, r4)));

        }
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> training = spark.createDataFrame(rowdata, schema);
//////////////////////Test Data
        List<Document> testSample = rdd.takeSample(false, 40000, 160000);
        List<Row> rowdata2 = new ArrayList<>();
        for (Document item : testSample) {
            String company = (String) item.get("vendor_id");
            double VTS = 0;
            if (company.equals("VTS")) {
                VTS = 1;
            }
            String id = item.get("_id").toString();
            double pickup_hour = Double.parseDouble(item.get("pickup_datetime").toString().substring(11, 13));
            double pickup_minutes = Double.parseDouble(item.get("pickup_datetime").toString().substring(14, 16));
            double dropoff_hour = Double.parseDouble(item.get("dropoff_datetime").toString().substring(11, 13));
            double dropoff_minutes = Double.parseDouble(item.get("dropoff_datetime").toString().substring(14, 16));
            double length = 0;
            if (pickup_hour == dropoff_hour) {
                length = dropoff_minutes - pickup_minutes;
            } else {
                if (pickup_hour < dropoff_hour) {
                    double hour_diff = dropoff_hour - pickup_hour;
                    double minute_diff = dropoff_minutes - pickup_minutes;
                    length = hour_diff * 60 + minute_diff;
                }
                if (pickup_hour > dropoff_hour) {
                    double hours_to_midnight = 24 - pickup_hour;
                    double hours_from_midnight = dropoff_hour;
                    double hour_diff = hours_to_midnight + hours_from_midnight;
                    double minute_diff = dropoff_minutes - pickup_minutes;
                    length = hour_diff * 60 + minute_diff;
                }
            }
            double tolls_amount = Double.parseDouble(item.get("tolls_amount").toString());
            double rate_code = Double.parseDouble(item.get("rate_code").toString());
            double r1 = 0;
            double r2 = 0;
            double r3 = 0;
            double r4 = 0;
            if (rate_code == 1) {
                r1 = 1;
            }
            if (rate_code == 2) {
                r2 = 1;
            }
            if (rate_code == 3) {
                r3 = 1;
            }
            if (rate_code == 4) {
                r4 = 1;
            }

            double tip_amount = Double.parseDouble(item.get("tip_amount").toString());
            double trip_distance = Double.parseDouble(item.get("trip_distance").toString());

            rowdata2.add(RowFactory.create(VTS, Vectors.dense(trip_distance,
                    tolls_amount, length, tip_amount, r1, r2, r3, r4)));
        }
        StructType schema2 = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> testData = spark.createDataFrame(rowdata2, schema2);

//////Model
        // Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("label")
                .setFeaturesCol("features");
        RandomForestClassificationModel model = rf.fit(training);
        //figure out how long it takes
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");

        System.out.println("Model 1 was fit using parameters: " + model.parent().extractParamMap());

        // Make predictions.
        Dataset<Row> predictions = model.transform(testData);
        predictions.show(40);

        RandomForestClassificationSummary modelSummary = model.summary();
        System.out.println("accuracy, " + modelSummary.accuracy());
        System.out.println("Precision, " + modelSummary.weightedPrecision());
        System.out.println("Recall, " + modelSummary.weightedRecall());

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println(accuracy);
    }
}