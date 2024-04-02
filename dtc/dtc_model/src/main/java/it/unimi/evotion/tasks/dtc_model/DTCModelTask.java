package it.unimi.evotion.tasks.dtc_model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DTCModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    /*
    private String train;
    private String labelName;
    private String label;
    private int    labelIndex;
    private String resultPath;
    */
    private List<String> lcn = new ArrayList<>();
    //private String maxIter;
    //private String maxCat;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private static final int maxBins=32;
    private static final int maxDepth=5;
    private static final double minInfoGain=0.0;
    private static final String impurity="gini";
    //private static final String lossType="logistic";
    private Dataset<Row> df;    // input dataframe
    private PipelineModel model;
    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the Decision Tree Classification Model
     *
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

        this.logger = LogManager.getLogger(DTCModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 6)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.train = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.maxCat = (String) args[3];
        this.maxIter = (String) args[4];
        this.labelIndex = -1;
        this.resultPath = (String) args[5];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("DTCModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("DTCModelTask")
                    .getOrCreate();
        }
    }
    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file

        // TODO: Fix the parameter. It should be called using csvData instead of training
        this.df = spark.read()
                //.format("csv")
                .format("csv") // was parquet
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        formatData();

// Fit on whole dataset to include all labels in index.
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setHandleInvalid("keep")
                .setOutputCol("indexedLabel")
                .fit(df);
// Automatically identify categorical features, and index them.
// Set maxCategories so features with > 4 distinct values are treated as continuous.
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(Integer.parseInt(parameters.get("maxCat")))
                .fit(df);

        // Train a DecisionTree model.
        DecisionTreeClassifier dt = new DecisionTreeClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxBins(Integer.parseInt(parameters.getOrDefault("maxBins", String.valueOf(maxBins))))
                .setMaxDepth(Integer.parseInt(parameters.getOrDefault("maxDepth", String.valueOf(maxDepth))))
                .setMinInfoGain(Double.parseDouble(parameters.getOrDefault("minInfoGain", String.valueOf(minInfoGain))))
                .setImpurity(parameters.getOrDefault("impurity", impurity));

/*
        System.out.println("maxBins " + gbt.getMaxBins());
        System.out.println("maxDepth " + gbt.getMaxDepth());
        System.out.println("MinInfoGain " + gbt.getMinInfoGain());
        System.out.println("impurity " + gbt.getImpurity());
*/
// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and GBT in a Pipeline.
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, dt, labelConverter});

// Train model. This also runs the indexers.
        this.model = pipeline.fit(this.df);
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.model.write().overwrite().save(parameters.get("resultPath"));
        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData(){
        /*StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(parameters.get("label"))))
                .setOutputCol("label");
*/
        String lbl = parameters.get("label"); //change

        //df = indexer.fit(df).transform(df);
        String processedLabelNames = parameters.get("labelName").replaceAll("'", "");
        for(String c : df.columns()) {
            df = df.withColumnRenamed(c, c.replaceAll("'", ""));
        }
        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s, %s as label FROM dft", processedLabelNames, lbl));
        this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
        String[] names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        df.show();

    }
}
