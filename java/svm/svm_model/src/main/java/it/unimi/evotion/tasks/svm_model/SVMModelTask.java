package it.unimi.evotion.tasks.svm_model;

import it.unimi.evotion.tasks.svm_model.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//TODO: Fix the package
public class SVMModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
/*
    private String csvData;
    private String labelName;
    private String label;
    private int    labelIndex;
    private String resultPath;
    private String maxIter;
    private String regParam;
    */
    private LinearSVCModel model;
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private static final int maxIter = 100;
    private static final boolean fitIntercept = true;
    private static final boolean standardization = true;
    private static final double regParam = 0.0;
    private static final double tol = 0.000001;
    private static final int aggDepth = 2;
    private static final double threshold = 0.0;

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
     *      args[1] vector variables
     *      args[2] label variable
     *      args[3] number of maximum iterations
     *      args[4] regression parameter
     *      args[5] url_json|url_csv result of SVM information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(SVMModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
/*
        if (args.length < 6)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.maxIter = (String) args[3];
        this.regParam = (String) args[4];
        this.labelIndex = -1;
        this.resultPath = (String) args[5];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("SVMModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("SVMModelTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                //.format("csv")
                .format("parquet")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        formatData();
        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter))))
                .setRegParam(Double.parseDouble(parameters.getOrDefault("regParam", String.valueOf(regParam))))
                .setStandardization(Boolean.parseBoolean(parameters.getOrDefault("standardization", String.valueOf(standardization))))
                .setTol(Double.parseDouble(parameters.getOrDefault("tol", String.valueOf(tol))))
                .setFitIntercept(Boolean.parseBoolean(parameters.getOrDefault("fitIntercept", String.valueOf(fitIntercept))))
                .setAggregationDepth(Integer.parseInt(parameters.getOrDefault("aggDepth", String.valueOf(aggDepth))))
                .setThreshold(Double.parseDouble(parameters.getOrDefault("threshold", String.valueOf(threshold))))
;
        // Fit the model
         this.model = lsvc.fit(df);

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