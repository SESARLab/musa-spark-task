package it.unimi.evotion.tasks.linreg;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class LinRegTask implements Task {

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
     * Create the Linear Regression Model
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] name|index (0-based) of the dependent (label) column/variable
     *      args[2] name|index (0-based) of the dependent (label) column/variable
     *      args[3] regression type: linear regression (lr) or generalized linear regression(glr)
     *      args[4] url_json|url_csv result of Linear Regression information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(LinRegTask.class);

        if (args.length < 5)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
        this.linRegType = (String) args[3];

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LinReg")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LinReg")
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

       // df = df.withColumn("label1", regexp_replace(df.col(this.label), "'", ""));
       // df = df.withColumn("label", (df.col("label1")
                //.cast("Double")
       // ))
        //        .drop("label1");
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

        convertFeatures();
        if(this.linRegType.equals("glr")){
            GeneralizedLinearRegression();
        }
        else if (this.linRegType.equals("lr")){
            LinearRegression();
        }
        else{
            throw new InvalidParameterException("Invalid parameter input received.");
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

        this.logger.info(this.debugString);

        this.spark.stop();
        this.spark.close();
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

    private void GeneralizedLinearRegression() {
        GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
                .setFamily("gaussian")
                .setLink("identity")
                .setMaxIter(20)
                .setRegParam(0.3);

        // Fit the model
        GeneralizedLinearRegressionModel model = glr.fit(dfa);
        // Summarize the model over the training set and print out some metrics
        GeneralizedLinearRegressionTrainingSummary summary = model.summary();

        List<Row> rows = Arrays.asList(
                RowFactory.create(model.intercept(), summary.dispersion(), summary.nullDeviance(),
                        summary.residualDegreeOfFreedomNull(), summary.deviance(), summary.residualDegreeOfFreedom(),
                        summary.aic()));

        System.out.println(rows);

        StructType schema;
        schema = new StructType(new StructField[]{
                new StructField("Intercept", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Dispersion", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Null_Deviance", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Residual_Degree_Of_Freedom Null", DataTypes.LongType, true, Metadata.empty()),
                new StructField("Deviance", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Residual_Degree_Of_Freedom", DataTypes.LongType, true, Metadata.empty()),
                new StructField("AIC", DataTypes.DoubleType, true, Metadata.empty()),
        });

        dfResult = spark.createDataFrame(rows, schema);
        dfResult2 = summary.residuals();


        StructType schema2;
        schema2 = new StructType(new StructField[]{
                new StructField("Coefficient_Standard_Errors", DataTypes.DoubleType, true, Metadata.empty())
                ,new StructField("T_Values", DataTypes.DoubleType, true, Metadata.empty())
                ,new StructField("P_Values", DataTypes.DoubleType, true, Metadata.empty())
        });

        List<Row> rowsToAdd = new ArrayList<>();
        for(int i = 0; i < summary.tValues().length; i++){
            rowsToAdd.add(RowFactory.create(summary.tValues()[i],summary.pValues()[i],summary.coefficientStandardErrors()[i]));
        }

        dfResult3 = spark.createDataFrame(rowsToAdd, schema2);
        dfResult3.show();

        StructType schema3 = new StructType(new StructField[]{
                new StructField("Coefficients", DataTypes.DoubleType, true, Metadata.empty())
        }   );

        List<Row> rows3 = new ArrayList<>();

        String s = model.coefficients().toString();
        s = s.replace("[", "").replace("]", "");

        List<String> sl = Arrays.stream(s.split(",")).collect(Collectors.toList());
        List<Double> sli = new ArrayList<Double>(sl.size());

        for(String str : sl) {
            sli.add(Double.valueOf(str));
        }

        Double[] sla = sli.stream().toArray(Double[]::new);

        // double[] arr = sli.stream().mapToDouble(d -> d).toArray();
        List<Row> r = Arrays.asList(
                RowFactory.create(s));

        for(int i = 0; i < sla.length; i++){
            rows3.add(RowFactory.create(sla[i]));
        }

        dfResult4 = spark.createDataFrame(rows3, schema3);
        dfResult4.show();

    }

    private void LinearRegression() {
        LinearRegression lr = new LinearRegression()
                .setMaxIter(20)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        // Fit the model.
        LinearRegressionModel lrModel = lr.fit(dfa);
        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();


        List<Row> rowsn = Arrays.asList(
                RowFactory.create(lrModel.intercept(), trainingSummary.totalIterations(),
                        trainingSummary.rootMeanSquaredError(), trainingSummary.r2()));

        StructType scheman;
        scheman = new StructType(new StructField[]{
                new StructField("Intercept", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Number_Of_Iterations", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("RMSE", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("r2", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult = spark.createDataFrame(rowsn, scheman);
        dfResult2 = trainingSummary.residuals();

        StructType schema3 = new StructType(new StructField[]{
                new StructField("Coefficients", DataTypes.DoubleType, true, Metadata.empty())
        }   );

        List<Row> rows3 = new ArrayList<>();

        String s = lrModel.coefficients().toString();
        s = s.replace("[", "").replace("]", "");

        List<String> sl = Arrays.stream(s.split(",")).collect(Collectors.toList());
        List<Double> sli = new ArrayList<Double>(sl.size());

        for(String str : sl) {
            sli.add(Double.valueOf(str));
        }

        Double[] sla = sli.stream().toArray(Double[]::new);

        for(int i = 0; i < sla.length; i++){
            rows3.add(RowFactory.create(sla[i]));
        }

        dfResult4 = spark.createDataFrame(rows3, schema3);
        dfResult4.show();

        StructType schema2 = new StructType(new StructField[]{
                new StructField("Objective_History", DataTypes.DoubleType, true, Metadata.empty())
        }   );

        List<Row> r = new ArrayList<>();
        for(int i = 0; i < trainingSummary.objectiveHistory().length; i++){
            r.add(RowFactory.create(trainingSummary.objectiveHistory()[i]));
        }

        dfResult3 = spark.createDataFrame(r, schema2);
        dfResult3.show();

    }
}