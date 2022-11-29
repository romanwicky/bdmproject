package sparkML;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import com.google.common.collect.Lists;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.bson.Document;

import static java.util.Collections.singletonList;

public final class Regression {

    // Labeled and unlabeled instance types.
// Spark SQL can infer schema from Java Beans.
    public static class Document implements Serializable {
        private Long id;
        private String text;

        public Document(Long id, String text) {
            this.id = id;
            this.text = text;
        }

        public Long getId() { return this.id; }
        public void setId(Long id) { this.id = id; }

        public String getText() { return this.text; }
        public void setText(String text) { this.text = text; }
    }

    public class LabeledDocument extends Document implements Serializable {
        private Double label;

        public LabeledDocument(Long id, String text, Double label) {
            super(id, text);
            this.label = label;
        }

        public Double getLabel() { return this.label; }
        public void setLabel(Double label) { this.label = label; }
    }

    SparkSession spark = SparkSession.builder()
            .master("local")
            .appName("MongoSparkConnectorIntro")
            .config("spark.mongodb.input.uri", "mongodb://localhost:27017/YellowTaxiCab.YTC")
            .config("spark.mongodb.output.uri", "mongodb://localhost:27017/YellowTaxiCab.YTC")
            .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    // Load and analyze data from MongoDB
    JavaMongoRDD<org.bson.Document> rdd = MongoSpark.load(jsc);
    SQLContext jsql = new SQLContext(jsc);

    // Prepare training documents, which are labeled.
    JavaRDD<Vector> inputData = org.bson.Document.get("vendor_id")
            .map(line -> {
                String[] parts = line.split(",");
                double[] v = new double[parts.length - 1];
                for (int i = 0; i < parts.length - 1; i++) {
                    v[i] = Double.parseDouble(parts[i]);
                }
                return Vectors.dense(v);
            });
    Dataset<Row> training =
            jsql.applySchema(jsc.parallelize(localTraining), LabeledDocument.class);

    // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
    Tokenizer tokenizer = new Tokenizer()
            .setInputCol("text")
            .setOutputCol("words");
    HashingTF hashingTF = new HashingTF()
            .setNumFeatures(1000)
            .setInputCol(tokenizer.getOutputCol())
            .setOutputCol("features");
    LogisticRegression lr = new LogisticRegression()
            .setMaxIter(10)
            .setRegParam(0.01);
    Pipeline pipeline = new Pipeline()
            .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});

    // Fit the pipeline to training documents.
    PipelineModel model = pipeline.fit(training);

    // Prepare test documents, which are unlabeled.
    List<Document> localTest = Lists.newArrayList(
            new Document(4L, "spark i j k"),
            new Document(5L, "l m n"),
            new Document(6L, "mapreduce spark"),
            new Document(7L, "apache hadoop"));
    Dataset<Row> test =
            jsql.applySchema(jsc.parallelize(localTest), Document.class);

/*// Make predictions on test documents.
model.transform(test).registerAsTable("prediction");
    Dataset<Row> predictions = jsql.sql("SELECT id, text, score, prediction FROM prediction");
for (Row r: predictions.collect())
    {
        System.out.println("(" + r.get(0) + ", " + r.get(1) + ") --> score=" + r.get(2)
                + ", prediction=" + r.get(3));
    }*/
}
