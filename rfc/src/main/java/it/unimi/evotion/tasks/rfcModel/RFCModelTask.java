package it.unimi.evotion.tasks.rfcModel;

import it.unimi.evotion.tasks.rfcModel.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
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

// TODO: Use the correct package
public class RFCModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    /*
    private String labelName;
    private String label;
    private String train;
    private int    labelIndex;
    private String resultPath;
    */
    private PipelineModel model;
    private List<String> lcn = new ArrayList<>();
    //private String maxCat;
    private Dataset<Row> df;    // input dataframe
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    public static final int maxBins=32;
    public static final int maxDepth=5;
    public static final double minInfoGain=0.0;
    public static final String fss="auto";

    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the Random Forest Classification Model
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

        this.logger = LogManager.getLogger(RFCModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));

  /*      if (args.length < 5)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.train = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.labelIndex = -1;
        this.maxCat = (String) args[3];
        this.resultPath = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("RFCModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("RFCModelTask")
                    .getOrCreate();
        }
    }
    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                .format("parquet")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("training"));

        formatData();
        StringIndexerModel labelIndexer = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indexedLabel")
                .fit(df);

        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(Integer.parseInt(parameters.get("maxCat")))  //4
                .fit(df);

// Train a RandomForest model.
        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("indexedLabel")
                .setFeaturesCol("indexedFeatures")
                .setMaxBins(Integer.parseInt(parameters.getOrDefault("maxBins", String.valueOf(maxBins))))
                .setMaxDepth(Integer.parseInt(parameters.getOrDefault("maxDepth", String.valueOf(maxDepth))))
                .setMinInfoGain(Double.parseDouble(parameters.getOrDefault("minInfoGain", String.valueOf(minInfoGain))))
                .setFeatureSubsetStrategy(parameters.getOrDefault("fss", fss));

// Convert indexed labels back to original labels.
        IndexToString labelConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictedLabel")
                .setLabels(labelIndexer.labels());

// Chain indexers and forest in a Pipeline
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

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

        df = indexer.fit(df).transform(df);
        */
        String lbl = parameters.get("label"); //change

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
