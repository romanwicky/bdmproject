package sparkML;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import static java.util.Collections.min;

public final class RegressionFareAmt {

    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/YellowTaxiCab.REGRESSIONDATASET")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/YellowTaxiCab.REGRESSIONDATASET")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // Load and analyze data from MongoDB
        long startTime = System.nanoTime();
        JavaMongoRDD<org.bson.Document> rdd = MongoSpark.load(jsc);

        List<Document> sample = rdd.takeSample(false, 20000, 0);
        //sample as big as memory put thru network, delete sample from memory .clear, grab another sample
        //repeat (see if you can .fit multiple times)...get multiple rdd.take samples and see how much you can get in row data
        //try different sample sizes and see if there are differeces in accuracy
        //any other experimentation
        List<Row> rowdata = new ArrayList<>();
        for (Document item : sample) {
            String company = (String) item.get("vendor_id");
            double CMT = 0;
            double VTS = 0;
            if (company.equals("VTS")){
                VTS = 1;
            }
            if (company.equals("CMT")){
                CMT = 1;
            }
            double pickup_hour = Double.parseDouble(item.get("pickup_datetime").toString().substring(11, 13));
            double pickup_minutes = Double.parseDouble(item.get("pickup_datetime").toString().substring(14, 16));
            double dropoff_hour = Double.parseDouble(item.get("dropoff_datetime").toString().substring(11, 13));
            double dropoff_minutes = Double.parseDouble(item.get("dropoff_datetime").toString().substring(14, 16));
            double morning = 0;
            double afternoon = 0;
            double night = 0;
            double lateNight = 0;
            double length = 0;
            if (pickup_hour >= 5 && pickup_hour < 11) {
                morning = 1;
            }
            if (pickup_hour >= 11 && pickup_hour < 17) {
                afternoon = 1;
            }
            if (pickup_hour >= 17 && pickup_hour < 23) {
                night = 1;
            }
            if (pickup_hour >= 23 || pickup_hour < 5) {
                lateNight = 1;
            }
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
            double trip_distance = Double.parseDouble(item.get("trip_distance").toString());
            double total_amount = Double.parseDouble(item.get("total_amount").toString());

            rowdata.add(RowFactory.create(total_amount, Vectors.dense(trip_distance,tolls_amount,
                    length, r1, r2, r3, r4)));
        }
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> training = spark.createDataFrame(rowdata, schema);
//////////////////////Test Data
        List<Document> testSample = rdd.takeSample(false, 80000, 320000);
        List<Row> rowdata2 = new ArrayList<>();
        for (Document item : testSample) {
            String company = (String) item.get("vendor_id");
            double CMT = 0;
            double VTS = 0;
            if (company.equals( "VTS")){
                VTS = 1;
            }
            if (company.equals("CMT")){
                CMT = 1;
            }
            double pickup_hour = Double.parseDouble(item.get("pickup_datetime").toString().substring(11, 13));
            double pickup_minutes = Double.parseDouble(item.get("pickup_datetime").toString().substring(14, 16));
            double dropoff_hour = Double.parseDouble(item.get("dropoff_datetime").toString().substring(11, 13));
            double dropoff_minutes = Double.parseDouble(item.get("dropoff_datetime").toString().substring(14, 16));
            double morning = 0;
            double afternoon = 0;
            double night = 0;
            double lateNight = 0;
            double length = 0;
            if (pickup_hour >= 5 && pickup_hour < 11) {
                morning = 1;
            }
            if (pickup_hour >= 11 && pickup_hour < 17) {
                afternoon = 1;
            }
            if (pickup_hour >= 17 && pickup_hour < 23) {
                night = 1;
            }
            if (pickup_hour >= 23 || pickup_hour < 5) {
                lateNight = 1;
            }
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
            double trip_distance = Double.parseDouble(item.get("trip_distance").toString());
            double total_amount = Double.parseDouble(item.get("total_amount").toString());

            rowdata2.add(RowFactory.create(total_amount, Vectors.dense(trip_distance,tolls_amount,
                    length, r1, r2, r3, r4)));
        }
        StructType schema2 = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty()),
        });
        Dataset<Row> testData = spark.createDataFrame(rowdata2, schema2);

//Model
        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // Fit the model.
        LinearRegressionModel lrModel = lr.fit(training);

        //figure out how long it takes
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");

// Print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show();
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());

        Dataset<Row> predictionsLR = lrModel.transform(testData);
        predictionsLR.show(40);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("r2");
        double r2 = evaluator.evaluate(predictionsLR);
        System.out.println("R squared Test, " + r2);

    }
}

