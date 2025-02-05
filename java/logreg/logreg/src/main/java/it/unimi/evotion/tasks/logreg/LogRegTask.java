package it.unimi.evotion.tasks.logreg;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.BinaryLogisticRegressionTrainingSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.stream.Collectors;

public class LogRegTask implements Task {

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
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> df2;
    private Dataset<Row> df3;
    private Dataset<Row> dfa;
    private long numberOfLabels;
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Dataset<Row> dfResult3;
    private Dataset<Row> dfResult4;
    private Dataset<Row> dfResult5;
    private Dataset<Row> dfResult6;
    private String elasticNetParam;
    private String regParam;
    private String maxIter;


    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the Logistic Regression Model
     *
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] name|index (0-based) of the dependent (label) column/variable
     *      args[2] url_json|url_csv result of Logistic Regression information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(LogRegTask.class);

        if (args.length < 7)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.maxIter = (String) args[3];
        this.regParam = (String) args[4];
        this.elasticNetParam = (String) args[5];
        this.labelIndex = -1;
        this.resultPath = (String) args[6];
       // this.linRegType = (String) args[4];

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LogReg")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LogReg")
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

        this.numberOfLabels = dfa.select("label").distinct().count();

    if(numberOfLabels>2) {
        MultivariateLogisticRegression();
    }
    else if (numberOfLabels==2){
        BinomialLogisticRegression();
    }
    else{
        throw new InvalidParameterException("Insufficient number of distinct labels");
    }

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

        if (this.numberOfLabels == 2){
            this.dfResult5.coalesce(1).write()
                    .format("csv")
                    .mode(SaveMode.Append)
                    .option("header", "true")
                    .save(resultPath);

            this.dfResult6.coalesce(1).write()
                    .format("csv")
                    .mode(SaveMode.Append)
                    .option("header", "true")
                    .save(resultPath);
        }

        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    private void MultivariateLogisticRegression() {
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(Integer.parseInt(this.maxIter)) //10
                .setRegParam(Double.parseDouble(this.regParam)) //0.3
                .setElasticNetParam(Double.parseDouble(this.elasticNetParam)); //0.8

// Fit the model
        LogisticRegressionModel lrModel = lr.fit(dfa);

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

        List<Row> collr = new ArrayList<>();

        int i = 0;
        List<Integer> labs = new ArrayList<>();
        double[] fprLabel = trainingSummary.falsePositiveRateByLabel();
        for (double fpr : fprLabel) {
      //      System.out.println("label " + i + ": " + fpr);
            labs.add(i);
            i++;
        }

        double[] tprLabel = trainingSummary.truePositiveRateByLabel();
        double[] precLabel = trainingSummary.precisionByLabel();
        double[] recLabel = trainingSummary.recallByLabel();
        double[] fLabel = trainingSummary.fMeasureByLabel();

        double accuracy = trainingSummary.accuracy();
        double falsePositiveRate = trainingSummary.weightedFalsePositiveRate();
        double truePositiveRate = trainingSummary.weightedTruePositiveRate();
        double fMeasure = trainingSummary.weightedFMeasure();
        double precision = trainingSummary.weightedPrecision();
        double recall = trainingSummary.weightedRecall();

        List<Row> rows1 = Arrays.asList(
                RowFactory.create(lrModel.coefficientMatrix().toString(), accuracy, falsePositiveRate,
                        truePositiveRate, fMeasure, precision, recall));

        StructType schema1 = new StructType(new StructField[]{
                new StructField("Coefficients", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Accuracy", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("FPR", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("TPR", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("F-measure", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Precision", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Recall", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult = spark.createDataFrame(rows1, schema1);


        StructType schema2 = new StructType(new StructField[]{
           new StructField("Loss_Per_Iteratioon", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult2 = spark.createDataFrame(lpi, schema2);

        StructType schema3 = new StructType(new StructField[]{
                new StructField("Intercepts", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult3 = spark.createDataFrame(intercept, schema3);


        StructType schema4 = new StructType(new StructField[]{
                new StructField("Label", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("FPR", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("TPR", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Precision", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Recall", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("F-measure", DataTypes.DoubleType, true, Metadata.empty())
        });

        for (int j = 0; j < fprLabel.length; j++){
            collr.add(RowFactory.create(labs.get(j), fprLabel[j],tprLabel[j],precLabel[j], recLabel[j], fLabel[j]));
        }
        dfResult4 = spark.createDataFrame(collr, schema4);
        dfResult4.show();
    }

    private void BinomialLogisticRegression() {
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(Integer.parseInt(this.maxIter))
                .setRegParam(Double.parseDouble(this.regParam))
                .setElasticNetParam(Double.parseDouble(this.elasticNetParam));

// Fit the model
        LogisticRegressionModel lrModel = lr.fit(dfa);

// Print the coefficients and intercept for logistic regression
    //    System.out.println("Coefficients: "
      //          + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        double[] coef = lrModel.coefficients().toArray();
        List<Row> co = new ArrayList<>();
        for (double v1: coef){
            co.add(RowFactory.create(v1));
        }

        LogisticRegression mlr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8)
                .setFamily("multinomial");

// Fit the model
        LogisticRegressionModel mlrModel = mlr.fit(dfa);

// Print the coefficients and intercepts for logistic regression with multinomial family
        System.out.println("Multinomial coefficients: " + lrModel.coefficientMatrix()
                + "\nMultinomial intercepts: " + mlrModel.interceptVector());

        double[] mco = lrModel.coefficientMatrix().toArray();
        List<Row> mc = new ArrayList<>();
        for (double v1: mco){
            mc.add(RowFactory.create(v1));
        }

        double[] multin = mlrModel.interceptVector().toArray();
        List<Row> mi = new ArrayList<>();
        for (double v1: multin){
            mi.add(RowFactory.create(v1));
        }

        BinaryLogisticRegressionTrainingSummary trainingSummary = lrModel.binarySummary();

// Obtain the loss per iteration.
        double[] objectiveHistory = trainingSummary.objectiveHistory();
        List<Row> lpi = new ArrayList<>();
        for (double lossPerIteration : objectiveHistory) {
            lpi.add(RowFactory.create(lossPerIteration));
        }

// Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
        Dataset<Row> roc = trainingSummary.roc();

// Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
// this selected threshold.
        Dataset<Row> fMeasure = trainingSummary.fMeasureByThreshold();
        double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
        double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
                .select("threshold").head().getDouble(0);
        lrModel.setThreshold(bestThreshold);

        dfResult = roc;

        List<Row> r2 = Arrays.asList(
                RowFactory.create(lrModel.intercept(), trainingSummary.areaUnderROC(), maxFMeasure, bestThreshold));

        StructType sch2 = new StructType(new StructField[]{
                new StructField("Intercept", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Area_Under_ROC", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Max_F-measure", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Best_Threshold", DataTypes.DoubleType, true, Metadata.empty()),
        });
        dfResult2 = spark.createDataFrame(r2, sch2);

        StructType sch3 = new StructType(new StructField[]{
                new StructField("Loss_Per_Iteration", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult3 = spark.createDataFrame(lpi, sch3);

        StructType sch4 = new StructType(new StructField[]{
                new StructField("Multinomial_Intercepts", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult4 = spark.createDataFrame(mi, sch4);

        StructType sch5 = new StructType(new StructField[]{
                new StructField("Coefficients", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult5 = spark.createDataFrame(co, sch5);

        StructType sch6 = new StructType(new StructField[]{
                new StructField("Multinomial_Coefficients", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult6 = spark.createDataFrame(mc, sch6);
    }

}