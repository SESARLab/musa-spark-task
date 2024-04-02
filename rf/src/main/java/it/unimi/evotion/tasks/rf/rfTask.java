package it.unimi.evotion.tasks.rf;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class rfTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private Dataset<Row> dfTree;
    private SparkSession spark;
    private String csvData;
    private String labelName;
    private String label;
    private String treePath;
    private int    labelIndex;
    private String resultPath;
    private String[] allNames;
    private String[] featureNames;
    private List<String> lcn = new ArrayList<>();
    private double testError;
    private boolean isRegression = true;
    //private Node rootNode;
    //private String rfType;
    private double rmse;
    private String trainSet;
    private String testSet;
    private String maxCat;

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;

    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the RandomForestTree Model
     *
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] user-defined features used for building the model
     *      args[2] name|index (0-based) of the dependent (label) column/variable
     *      args[3] url_json|url_csv result of DTree information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(rfTask.class);

        if (args.length < 6)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.labelIndex = -1;
        //this.rfType = (String) args[3];
        this.trainSet = (String) args[3];
        this.testSet = (String) args[4];
        this.maxCat = (String) args[5];
        this.resultPath = (String) args[6];
        this.treePath = ((String) args[6]) + ".tree";

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("rfTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("rfTask")
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
        //checkForRegression();
     /*   if (rfType.equals("C")){
            rfClassification();
        }
        else if (rfType.equals("R")) {
            rfRegression();
        }
        else {
            System.out.println("Invalid parameter selected.");
        }*/
        rfClassification();
        createResults();

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(resultPath);

        this.dfResult2.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.dfTree.write().text(treePath);
        this.logger.info(this.debugString);

        this.spark.stop();
        this.spark.close();
    }


    private void rfClassification() {

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(df);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(Integer.parseInt(this.maxCat))  //4
                .fit(df);

        Dataset<Row>[] splits = df.randomSplit(new double[] {Double.parseDouble(this.trainSet), Double.parseDouble(this.testSet)});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

// Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

// Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
        Dataset<Row> predictions = model.transform(testData);

// Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

// Select (prediction, true label) and compute test error
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        //System.out.println("Test Error = " + (1.0 - accuracy));

        RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);

        this.featureImportances = rfModel.featureImportances();
        this.debugString = rfModel.toDebugString();
        this.testError = 1 - accuracy;
    }

/*
    private void rfRegression(){
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(df);

// Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = df.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

// Train a RandomForest model.
        RandomForestRegressor rf = new RandomForestRegressor()
                .setLabelCol("label")
                .setFeaturesCol("indexedFeatures");

// Chain indexer and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {featureIndexer, rf});

// Train model. This also runs the indexer.
        PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
        Dataset<Row> predictions = model.transform(testData);

// Select example rows to display.
        predictions.select("prediction", "label", "features").show(5);

// Select (prediction, true label) and compute test error
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("rmse");
        this.rmse = evaluator.evaluate(predictions);
        System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);
        RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model.stages()[1]);
        //System.out.println("Learned regression forest model:\n" + rfModel.toDebugString());
       // System.out.println(rfModel.featureImportances());
        this.featureImportances = rfModel.featureImportances();
        this.debugString = rfModel.toDebugString();

    }
*/

    private void createResults(){
        int[] indices = ((SparseVector) featureImportances).indices();
        double[] values = ((SparseVector) featureImportances).values();
        List<rfRecord> lfm = new ArrayList<>();

        StructType schema = this.df.schema();
        allNames = schema.fieldNames();

        for(int i=0; i<indices.length; ++i) {
            String name = allNames[indices[i]];
            double value = values[i];
            lfm.add(new rfRecord(name, value));
        }

        List<rfTreeStruct> tree = new ArrayList<>();
        // this.debugString = rfModel.toDebugString();
        tree.add(new rfTreeStruct(debugString));

        List<Row> rows = Arrays.asList(
                RowFactory.create(testError));

        StructType sch = new StructType(new StructField[]{
                new StructField("Test_Error", DataTypes.DoubleType, true, Metadata.empty()),
        });

    /*    List<Row> rows2 = Arrays.asList(
                RowFactory.create(rmse));

        StructType sch2 = new StructType(new StructField[]{
                new StructField("RMSE", DataTypes.DoubleType, true, Metadata.empty()),
        });
*/
        dfResult = spark.createDataFrame(lfm, rfRecord.class);
        dfTree = spark.createDataFrame(tree, rfTreeStruct.class);
        //if (rfType.equals("C")){
            dfResult2 = spark.createDataFrame(rows, sch);
        /*}
        else if (rfType.equals("R")) {
            dfResult2 = spark.createDataFrame(rows2, sch2);
        }*/

    }

    void formatData(){
        StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(this.label)))
                .setOutputCol("label");

        df = indexer.fit(df).transform(df);
        //dfi.show();

        String processedLabelNames = this.labelName.replaceAll("'", "");
        for(String c : df.columns()) {
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
        //dfa.show();
        df.show();
    }

    private void convertFeatures() {
        List<String> stringFeatures = new ArrayList<>();

        for(StructField field : this.df.schema().fields()) {
            if (field.dataType().equals(DataTypes.StringType))
                stringFeatures.add(field.name());
        }

        for(String sf : stringFeatures) {
            String rn = "str-"+sf;
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
