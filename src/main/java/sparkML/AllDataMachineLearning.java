package sparkML;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassificationSummary;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

public class AllDataMachineLearning {
    public static void main(final String[] args) throws InterruptedException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/YellowTaxiCab.TRAINRATECODEBIG")
                .config("spark.mongodb.output.uri", "mongodb://localhost:27017/YellowTaxiCab.TRAINRATECODEBIG")
                .getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        // Load and analyze data from MongoDB
        long startTime = System.nanoTime();
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { \"rate_code\": { $lt: \"5\" }}}")));

        JavaRDD<Document>[] split = aggregatedRdd.randomSplit(new double[]{.8, .2}, 100);
        JavaRDD<Document> trainingSample = split[0].cache();
        JavaRDD<Document> testingSample = split[1];

        JavaRDD<Row> jrdd = trainingSample.map(f-> RowFactory.create((Double.parseDouble(f.get("rate_code").toString())),
                Vectors.dense(Double.parseDouble(f.get("trip_distance").toString()), Double.parseDouble(f.get("tip_amount").toString()),
                        Double.parseDouble(f.get("tolls_amount").toString()),Double.parseDouble(f.get("total_amount").toString()) )));
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> training = spark.createDataFrame(jrdd, schema);

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("label")
                .setFeaturesCol("features")
                ;
        RandomForestClassificationModel model = rf.fit(training);
        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " seconds");

        System.out.println("Model 1 was fit using parameters: " + model.parent().extractParamMap());
        RandomForestClassificationSummary modelSummary = model.summary();
        System.out.println("accuracy, " + modelSummary.accuracy());

        //Testing Data
        JavaRDD<Row> jrddTest = testingSample.map(f-> RowFactory.create((Double.parseDouble(f.get("rate_code").toString())),
                Vectors.dense(Double.parseDouble(f.get("trip_distance").toString()), Double.parseDouble(f.get("tip_amount").toString()),
                        Double.parseDouble(f.get("tolls_amount").toString()),Double.parseDouble(f.get("total_amount").toString()) )));
        StructType schemaTest = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> testing = spark.createDataFrame(jrddTest, schemaTest);
        // Make predictions.
        Dataset<Row> predictions = model.transform(testing);
        predictions.show(40);

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println(accuracy);

        //        //Model
//        LinearRegression lr = new LinearRegression()
//                .setMaxIter(10)
//                .setRegParam(0.3)
//                .setElasticNetParam(0.8);
//
//        // Fit the model.
//        LinearRegressionModel lrModel = lr.fit(training);
//
//        //figure out how long it takes
//        long stopTime = System.nanoTime();
//        long elapsedTime = stopTime - startTime;
//        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS);
//        System.out.println(convert + " seconds");
//
//// Print the coefficients and intercept for linear regression.
//        System.out.println("Coefficients: "
//                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
//
//        // Summarize the model over the training set and print out some metrics.
//        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
//        System.out.println("numIterations: " + trainingSummary.totalIterations());
//        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
//        trainingSummary.residuals().show();
//        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
//        System.out.println("r2: " + trainingSummary.r2());
//
//        JavaRDD<Row> jrddTest = testingSample.map(f-> RowFactory.create((Double.parseDouble(f.get("rate_code").toString())),
//                Vectors.dense(Double.parseDouble(f.get("trip_distance").toString()), Double.parseDouble(f.get("tip_amount").toString()),
//                        Double.parseDouble(f.get("tolls_amount").toString()),Double.parseDouble(f.get("total_amount").toString()) )));
//        StructType schemaTest = new StructType(new StructField[]{
//                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
//                new StructField("features", new VectorUDT(), false, Metadata.empty())
//        });
//        Dataset<Row> testing = spark.createDataFrame(jrddTest, schemaTest);
//        Dataset<Row> predictionsLR = lrModel.transform(testing);
//        predictionsLR.show(40);
//
//        RegressionEvaluator evaluator = new RegressionEvaluator()
//                .setLabelCol("label")
//                .setPredictionCol("prediction")
//                .setMetricName("r2");
//        double r2 = evaluator.evaluate(predictionsLR);
//        System.out.println("R squared Test, " + r2);
    }
}
