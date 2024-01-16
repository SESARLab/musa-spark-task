package com.example.nbayes;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class nBayesTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private String csvData;
    private String labelName;
    private String label;
    private int labelIndex;
    private String resultPath;
    private String trainSet;
    private String testSet;
    //private String[] allNames;
    //private String[] featureNames;
    private List<String> lcn = new ArrayList<>();
    private double testError;

    private double rmse;


    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> predictions;

    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    /**
     * Create the RandomForestTree Model
     * <p>
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args args[0] url_csv input dataset (in CSV with header format)
     *             args[1] user-defined features used for building the model
     *             args[2] name|index (0-based) of the dependent (label) column/variable
     *             args[3] training set size
     *             args[4] test set size
     *             args[5] url_json|url_csv result of naive bayes information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(nBayesTask.class);

        if (args.length < 4)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.trainSet = (String) args[3];
        this.testSet = (String) args[4];
        this.labelIndex = -1;
        this.resultPath = (String) args[5];

        try {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    // .master("yarn")
                    .appName("nBayesTask")
                    .getOrCreate();
        } catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    // .master("yarn")
                    .appName("nBayesTask")
                    .getOrCreate();
        }
    }


    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvData);

        convertFeatures();
        formatData();
        Dataset<Row>[] splits = df.randomSplit(new double[]{Double.parseDouble(this.trainSet), Double.parseDouble(this.testSet)}, 1234L);
        Dataset<Row> train = splits[0];
        Dataset<Row> test = splits[1];

// create the trainer and set its parameters
        NaiveBayes nb = new NaiveBayes();

// train the model
        NaiveBayesModel model = nb.fit(train);
       // VecToString vecstr = new VecToString();

// Select example rows to display.
        this.predictions = model.transform(test);
        //  predictions.show();
        predictions = predictions
                .drop("Features")
                //.withColumn("raw_prediction", col("rawPrediction"))
                //.withColumn("probability", concat_ws(",", col("probability")).cast(DataTypes.StringType))
                .drop("rawPrediction")
                .drop("probability");


       // predictions.show();

       // predictions.createOrReplaceTempView("pred");
       // predictions = spark.sql(String.format("SELECT features, rawPrediction, probability, label, prediction FROM pred"));
       // predictions = spark.sql(String.format("SELECT label, CAST(features AS ARRAY<STRING>), CAST(rawPrediction AS STRING), CAST(probability AS STRING), prediction FROM pred"));


        //predictions = predictions.selectExpr("CAST(features AS ARRAY<STRING>)").selectExpr("CAST(rawPrediction AS ARRAY<STRING>)").selectExpr("CAST(probability AS ARRAY<STRING>)").select("prediction");
        //   , CAST(rawPrediction AS ARRAY<STRING>), CAST(probability AS ARRAY<STRING>), label, prediction")
        predictions.show();
// compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);

        List<Row> row = Arrays.asList(
                RowFactory.create(accuracy));

        StructType sch = new StructType(new StructField[]{
                new StructField("Test_Set_Accuracy", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult = spark.createDataFrame(row, sch);

        System.out.println("Test set accuracy = " + accuracy);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(resultPath);

        this.predictions.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData() {
        StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(this.label)))
                .setOutputCol("label");

        df = indexer.fit(df).transform(df);

        String processedLabelNames = this.labelName.replaceAll("'", "");
        for (String c : df.columns()) {
            df = df.withColumnRenamed(c, c.replaceAll("'", ""));
        }
        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s, label FROM dft", processedLabelNames));


        this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
        String[] names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        df.show();
    }

    private void convertFeatures() {
        List<String> stringFeatures = new ArrayList<>();

        for (StructField field : this.df.schema().fields()) {
            if (field.dataType().equals(DataTypes.StringType))
                stringFeatures.add(field.name());
        }

        for (String sf : stringFeatures) {
            String rn = "str-" + sf;
            this.df = this.df.withColumnRenamed(sf, rn);
            StringIndexer encoder = new StringIndexer()
                    .setInputCol(rn)
                    .setOutputCol(sf);

            this.df = encoder.fit(this.df).transform(this.df).drop(rn);
            this.df = this.df.withColumn(sf, this.df.col(sf).cast(DataTypes.IntegerType));
        }

        this.df.printSchema();
        this.df.show();
    }

}
