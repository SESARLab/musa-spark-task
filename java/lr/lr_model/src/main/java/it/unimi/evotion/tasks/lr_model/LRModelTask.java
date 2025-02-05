package it.unimi.evotion.tasks.lr_model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class LRModelTask implements Task {

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
    private String elasticNetParam;
    */
    private List<String> lcn = new ArrayList<>();
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Dataset<Row> dfResult3;
    private Dataset<Row> dfResult4;
    private LinearRegressionModel model;
    private static final boolean fitIntercept=true;
    private static final boolean standardization=true;
    private static final int aggDepth=2;
    private static final double tol=0.000001;
    private static final double elasticNetParam=0.0;
    private static final double regParam=0.0;
    private static final int maxIter=100;
    private Vector featureImportances;
    private String debugString;
    private Dataset<Row> dfResult22;
    private String[] names;
    private Dataset<Row> dfr;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the Linear Regression Model
     *
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] name|index (0-based) of the dependent (label) column/variable
     *      args[2] name|index (0-based) of the dependent (label) column/variable
     *      args[3] number of maximum iterations
     *      args[4] regression parameter
     *      args[5] url_json|url_csv result of Linear Regression information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(LRModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 7)
            throw new InvalidParameterException("Missing parameters");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.maxIter = (String) args[3];
        this.regParam = (String) args[4];
        this.elasticNetParam = (String) args[5];
        this.labelIndex = -1;
        this.resultPath = (String) args[6];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LRModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LRModelTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                .format("csv") // was parquet
                //.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        formatData();
        LinearRegression();
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.model.write().overwrite().save(parameters.get("resultPath"));

        this.dfr
                .coalesce(1)
                .write()
                //.format("csv")
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath")+"/intercept");

        this.dfResult2
                .coalesce(1)
                .write()
                //.format("csv")
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath")+"/residuals");

        this.dfResult3
                .coalesce(1)
                .write()
                //.format("csv")
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath")+"/objectiveHistory");

    /*    this.dfResult4
                //.coalesce(1)
                .write()
                //.format("csv")
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath")+"/coefficients");
    */
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
        //Check if label is contained in labelName and remove it if yes
        String pln[] =processedLabelNames.split(",") ;
        List<String> lpln = new ArrayList<>();
        for (int i = 0; i < pln.length; i++){
            lpln.add(pln[i]);
        }
        if(lpln.contains(String.format("%s", parameters.get("label")))) {
            lpln.remove(String.format("%s", parameters.get("label")));
        }
        String str2 = String.join(",", lpln );

        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s, %s as label FROM dft", str2, lbl));
        this.lcn = Arrays.stream(str2.split(",")).collect(Collectors.toList());
        names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        df.show();
    }

    public Double lci(Double c, Double se){
        return c - 1.96 * se;
    }
    public Double uci(Double c, Double se){
        return c + 1.96 * se;
    }

    private void LinearRegression() {

        LinearRegression lr = new LinearRegression()
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter))))
                .setRegParam(Double.parseDouble(parameters.getOrDefault("regParam", String.valueOf(regParam))))
                .setElasticNetParam(Double.parseDouble(parameters.getOrDefault("elasticNetParam", String.valueOf(elasticNetParam))))
                .setFitIntercept(Boolean.parseBoolean(parameters.getOrDefault("fitIntercept", String.valueOf(fitIntercept))))
                .setAggregationDepth(Integer.parseInt(parameters.getOrDefault("aggDepth", String.valueOf(aggDepth))))
                .setTol(Double.parseDouble(parameters.getOrDefault("tol", String.valueOf(tol))))
                .setStandardization(Boolean.parseBoolean(parameters.getOrDefault("standardization", String.valueOf(standardization))));
        // Fit the model.
        model = lr.fit(this.df);
        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = model.summary();
        int nsize = names.length;
        int psize = trainingSummary.pValues().length;
        for(int i=0; i<psize-1; i++)
        {
            System.out.println("p-value_" + names[i] + " : " + trainingSummary.pValues()[i]);
            System.out.println("t-value_" + names[i] + " : " + trainingSummary.tValues()[i]);
        }

        List<Row> rowsn = Arrays.asList(
                RowFactory.create(model.intercept(), trainingSummary.totalIterations(),
                        trainingSummary.rootMeanSquaredError(), trainingSummary.r2()
                        , trainingSummary.pValues()[0]
                        , trainingSummary.pValues()[psize-1]
                        ,/* trainingSummary.r2adj(),*/ trainingSummary.meanAbsoluteError(), trainingSummary.meanSquaredError()
                        , trainingSummary.tValues()[0]
                        , trainingSummary.tValues()[psize-1]
                        , trainingSummary.numInstances()
                ));

       // List<Double> rowlist = Arrays.asList(model.intercept(), (double) trainingSummary.totalIterations(), trainingSummary.rootMeanSquaredError(), trainingSummary.r2(),
         //       trainingSummary.meanAbsoluteError(), trainingSummary.meanSquaredError(), (double) trainingSummary.numInstances());

        //values

        List<Double> rowlist = new ArrayList<>();

        rowlist.add(model.intercept());
        rowlist.add((double) trainingSummary.totalIterations());
        rowlist.add(trainingSummary.rootMeanSquaredError());
        rowlist.add(trainingSummary.r2());
        rowlist.add(trainingSummary.meanAbsoluteError());
        rowlist.add(trainingSummary.meanSquaredError());
        rowlist.add((double) trainingSummary.numInstances());
        rowlist.add(trainingSummary.pValues()[psize-1]);
        rowlist.add(trainingSummary.tValues()[psize-1]);
        rowlist.add(trainingSummary.coefficientStandardErrors()[psize-1]);
        rowlist.add(lci(model.intercept(), trainingSummary.coefficientStandardErrors()[psize-1]));
        rowlist.add(uci(model.intercept(), trainingSummary.coefficientStandardErrors()[psize-1]));

        String s = model.coefficients().toString();
        s = s.replace("[", "").replace("]", "");
        List<String> sl = Arrays.stream(s.split(",")).collect(Collectors.toList());
        List<Double> sli = new ArrayList<Double>(sl.size());

        for(String str : sl) {
            sli.add(Double.valueOf(str));
        }

        Double[] sla = sli.stream().toArray(Double[]::new);

        for(int i=0; i<psize-1; i++)
        {
            rowlist.add(trainingSummary.pValues()[i]);
            rowlist.add(trainingSummary.tValues()[i]);
            rowlist.add(sli.get(i));
            rowlist.add(trainingSummary.coefficientStandardErrors()[i]);
            rowlist.add(lci(sli.get(i), trainingSummary.coefficientStandardErrors()[i]));
            rowlist.add(uci(sli.get(i), trainingSummary.coefficientStandardErrors()[i]));
        }

        //labels
        List<String> rownames = new ArrayList<>();
        rownames.add("Intercept");
        rownames.add("Number_Of_Iterations");
        rownames.add("RMSE");
        rownames.add("r2");
        rownames.add("Mean_Absolute_Error");
        rownames.add("Mean_Squared_Error");
        rownames.add("Number_of_Observations");
        rownames.add("p_Value_Intercepts");
        rownames.add("t_Value_Intercepts");
        rownames.add("Standard_Error_Intercept");
        rownames.add("Lower_95%_Intercept");
        rownames.add("Upper_95%_Intercept");

        for(int i=0; i<psize-1; i++)
        {
            rownames.add("p_Value_"+ names[i]);
            rownames.add("t_Value_"+ names[i]);
            rownames.add("Coefficient_"+ names[i]);
            rownames.add("Standard_Error_"+ names[i]);
            rownames.add("Lower_95%_"+ names[i]);
            rownames.add("Upper_95%_"+ names[i]);
        }


        List<String> rowtype= new ArrayList<>();
        for (int i = 0; i < rownames.size(); i++){
            rowtype.add(rownames.get(i)+"_"+(i+1));
        }

        List<Row> rowsr = new ArrayList<>();
        for(int j = 0; j <rownames.size(); j++){
            rowsr.add(RowFactory.create(rownames.get(j), rowtype.get(j), rowlist.get(j)));
        }

        StructType scheman2;
        scheman2= new StructType(new StructField[]{
                new StructField("Paramnames", DataTypes.StringType, true, Metadata.empty()),
                new StructField("TYPE", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Values", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult = spark.createDataFrame(rowsr, scheman2);
        dfResult.show(false);
        dfResult = spark.createDataFrame(rowsr, scheman2);
        dfResult.createOrReplaceTempView("table");
        dfr = spark.sql("SELECT REPLACE(Paramnames, '_CASTED', '') AS Paramnames, REPLACE(TYPE, '_CASTED', '') AS Type, REPLACE(Values, '_CASTED', '') AS Values FROM table");
        dfResult2 = trainingSummary.residuals();
//coefficients
     /*   StructType schema3 = new StructType(new StructField[]{
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

        for(int i = 0; i < sla.length; i++){
            rows3.add(RowFactory.create(sla[i]));
        }

        dfResult4 = spark.createDataFrame(rows3, schema3);
//coefficients end
      */
        StructType schema2 = new StructType(new StructField[]{
                new StructField("Objective_History", DataTypes.DoubleType, true, Metadata.empty())
        }   );

        List<Row> r = new ArrayList<>();
        for(int i = 0; i < trainingSummary.objectiveHistory().length; i++){
            r.add(RowFactory.create(trainingSummary.objectiveHistory()[i]));
        }

        dfResult3 = spark.createDataFrame(r, schema2);

    }
}