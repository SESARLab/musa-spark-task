package it.unimi.evotion.tasks.dtc;

import it.unimi.evotion.tasks.dtc.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class dtcTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private String csvData;
    private String labelName;
    private String label;
    private int    labelIndex;
    private String resultPath;
    private String linRegType;
    private String[] allNames;
    private String[] featureNames;
    private List<String> lcn = new ArrayList<>();

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> df2;
    private Dataset<Row> df3;
    private Dataset<Row> dfa;

    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Dataset<Row> dfResult3;
    private Dataset<Row> dfResult4;

    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the DecisionTree Model
     *
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] name|index (0-based) of the dependent (label) column/variable
     *      args[2] url_json|url_csv result of DTree information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(dtcTask.class);

        if (args.length < 4)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.labelIndex = -1;
        this.resultPath = (String) args[3];
        // this.linRegType = (String) args[4];

        try {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("dtcTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("dtcTask")
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


        StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(this.label)))
                .setOutputCol("label");

        Dataset<Row> dfi = indexer.fit(df).transform(df);
        dfi.show();

        String processedLabelNames = this.labelName.replaceAll("'", "");

        df2 = dfi;
        for(String c : df2.columns()) {
            df2 = df2.withColumnRenamed(c, c.replaceAll("'", ""));
        }
        df2.createOrReplaceTempView("dft");
        df3 = spark.sql(String.format("SELECT %s, label FROM dft", processedLabelNames));


        this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
        String[] names = this.lcn.stream().toArray(String[]::new);


        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        dfa = assembler1.transform(df3);
        dfa.show();

        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(dfa);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(dfa);


        Dataset<Row>[] splits = dfa.randomSplit(new double[] {0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];


// Train a DecisionTree model.
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures");

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and tree in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

// Train model. This also runs the indexers.
        PipelineModel model = pipeline.fit(trainingData);

// Make predictions.
        Dataset<Row> predictions = model.transform(testData);

// Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5);

// Select (prediction, true label) and compute test error.
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test Error = " + (1.0 - accuracy));

        DecisionTreeClassificationModel treeModel =
                (DecisionTreeClassificationModel) (model.stages()[2]);
        System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
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

        this.dfResult3.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.dfResult4.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.logger.info(this.debugString);

        this.spark.stop();
        this.spark.close();
    }


}
