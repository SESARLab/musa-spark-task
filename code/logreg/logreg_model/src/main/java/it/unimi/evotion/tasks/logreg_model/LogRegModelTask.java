package it.unimi.evotion.tasks.logreg_model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

public class LogRegModelTask implements Task {

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
    */
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private long numberOfLabels;
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private static final double elasticNetParam=0.0;
    private static final double regParam=0.0;
    private static final int maxIter=100;
    private static final boolean fitIntercept=true;
    private static final boolean standardization=true;
    private static final double threshold=0.5;
    private static final int aggDepth=2;
    private static final double tol=0.000001;
    private LogisticRegressionModel lrModel;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(LogRegModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 12)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.maxIter = (String) args[3];  //20
        this.regParam = (String) args[4];  //0.3
        this.elasticNetParam = (String) args[5];  //0.8
        this.fitIntercept = (String) args[6];  //true
        this.standardization = (String) args[7];  //true
        this.threshold = (String) args[8];   //0.5
        this.aggregationDepth =(String) args[9];  //2
        this.tol =(String) args[10];  //1E-6

        this.labelIndex = -1;
        this.resultPath = (String) args[11];
       // this.linRegType = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LogRegModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LogRegModelTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                //.format("csv")
                .format("csv") // was parquet
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        formatData();
        this.numberOfLabels = df.select("label").distinct().count();

    if(numberOfLabels>2) {
        MultivariateLogisticRegression();
    }
    else if (numberOfLabels==2){
        BinomialLogisticRegression();
    }
    else{
        throw new InvalidParameterException("Insufficient number of distinct labels.");
    }
    }

    @Override
    public void postProcessing(Object... params) throws Exception {
        // TODO: Port the classes in the correct package
        this.lrModel.write().overwrite().save(parameters.get("resultPath"));

        this.dfResult
                     //.coalesce(1)
                     .write()
                     //.format("csv")
                    .format("parquet")
                    .mode(SaveMode.Append)
                    .option("header", "true")
                    .save(parameters.get("resultPath"));

            this.dfResult2
                    //.coalesce(1)
                    .write()
                    //.format("csv")
                    .format("parquet")
                    .mode(SaveMode.Append)
                    .option("header", "true")
                    .save(parameters.get("resultPath"));

        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    private void MultivariateLogisticRegression() {
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter)))) //10
                .setRegParam(Double.parseDouble(parameters.getOrDefault("regParam", String.valueOf(regParam)))) //0.3
                .setElasticNetParam(Double.parseDouble(parameters.getOrDefault("elasticNetParam", String.valueOf(elasticNetParam))))
                .setFitIntercept(Boolean.parseBoolean(parameters.getOrDefault("fitIntercept", String.valueOf(fitIntercept))))  //true
                .setStandardization(Boolean.parseBoolean(parameters.getOrDefault("standardization", String.valueOf(standardization))))  //true
                .setThreshold(Double.parseDouble(parameters.getOrDefault("threshold", String.valueOf(threshold))))  //0.5
                .setAggregationDepth(Integer.parseInt(parameters.getOrDefault("aggDepth", String.valueOf(aggDepth))))  //2
                .setTol(Double.parseDouble(parameters.getOrDefault("tol", String.valueOf(tol))));  //1E-6
// Fit the model
        lrModel = lr.fit(this.df);

        //lrModel.transform(this.df).show();

        LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();

        double[] intercepts = lrModel.interceptVector().toArray();
        List<Row> intercept = new ArrayList<>();

        for (double v1: intercepts){
            intercept.add(RowFactory.create(v1));
        }

        List<Row> lpi = new ArrayList<>();

// Obtain the loss per iteration.
        double[] objectiveHistory = trainingSummary.objectiveHistory();
        for (double lossPerIteration : objectiveHistory) {
            lpi.add(RowFactory.create(lossPerIteration));
        }

        StructType schema2 = new StructType(new StructField[]{
           new StructField("Loss_Per_Iteratioon", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult = spark.createDataFrame(lpi, schema2);

        StructType schema3 = new StructType(new StructField[]{
                new StructField("Intercepts", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult2 = spark.createDataFrame(intercept, schema3);
    }

    private void BinomialLogisticRegression() {
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter)))) //10
                .setRegParam(Double.parseDouble(parameters.getOrDefault("regParam", String.valueOf(regParam)))) //0.3
                .setElasticNetParam(Double.parseDouble(parameters.getOrDefault("elasticNetParam", String.valueOf(elasticNetParam))))
                .setFitIntercept(Boolean.parseBoolean(parameters.getOrDefault("fitIntercept", String.valueOf(fitIntercept))))  //true
                .setStandardization(Boolean.parseBoolean(parameters.getOrDefault("standardization", String.valueOf(standardization))))  //true
                .setThreshold(Double.parseDouble(parameters.getOrDefault("threshold", String.valueOf(threshold))))  //0.5
                .setAggregationDepth(Integer.parseInt(parameters.getOrDefault("aggDepth", String.valueOf(aggDepth))))  //2
                .setTol(Double.parseDouble(parameters.getOrDefault("tol", String.valueOf(tol))));  //1E-6

// Fit the model
        lrModel = lr.fit(this.df);

        //lrModel.transform(this.df).show();

        double[] coef = lrModel.coefficients().toArray();
        List<Row> co = new ArrayList<>();
        for (double v1: coef){
            co.add(RowFactory.create(v1));
        }


        double[] mco = lrModel.coefficientMatrix().toArray();
        List<Row> mc = new ArrayList<>();
        for (double v1: mco){
            mc.add(RowFactory.create(v1));
        }

        StructType sch5 = new StructType(new StructField[]{
                new StructField("Coefficients", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult = spark.createDataFrame(co, sch5);

        StructType sch6 = new StructType(new StructField[]{
                new StructField("Multinomial_Coefficients", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult2 = spark.createDataFrame(mc, sch6);
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