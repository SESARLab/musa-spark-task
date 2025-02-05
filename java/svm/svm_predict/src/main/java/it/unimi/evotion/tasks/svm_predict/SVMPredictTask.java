package it.unimi.evotion.tasks.svm_predict;

import it.unimi.evotion.tasks.svm_predict.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.Vector;
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

// TODO: Fix the package
public class SVMPredictTask implements Task {

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
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> predictions;
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private String[] names;


    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(SVMPredictTask.class);
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
                    .appName("SVMPredictTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("SVMPredictTask")
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

        df.createOrReplaceTempView("t");
        df = spark.sql(String.format("SELECT * FROM t ORDER BY %s", parameters.get("constParams")));
        df.createOrReplaceTempView("v11");
        //Dataset<Row> df2 = spark.sql(String.format("SELECT row_number() over (order by %s DESC) as ID, %s FROM v11", parameters.get("constParams"), parameters.get("constParams")));
        Dataset<Row> df2 = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM v11", parameters.get("constParams"), parameters.get("constParams")));
        df2.show(false);
        df2.createOrReplaceTempView("v12");

        formatData();

        LinearSVCModel lsvcModel = LinearSVCModel.load(parameters.get("modelData"));
        predictions = lsvcModel.transform(this.df);
        predictions = predictions.drop("features");
        VectorDisassembler vd = new VectorDisassembler().setInputCol("rawPrediction");
        predictions = vd.transform(predictions).drop("rawPrediction");

        String[] predCols = predictions.columns();
        List<String> prCols = Arrays.asList(predCols);
        String snames = String.join(",", prCols);
/*
        List<String> lnames = Arrays.asList(names);
        String snames = String.join(",", lnames);*/
        predictions.createOrReplaceTempView("v21");
        //dfResult = spark.sql(String.format("SELECT row_number() over (order by %s DESC) as ID, %s FROM v21", snames, snames));
        predictions = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM v21", snames));
        predictions.show(false);
        predictions.createOrReplaceTempView("v22");
        predictions = spark.sql("SELECT * FROM v12, v22 WHERE v12.ID=v22.ID").drop("ID");
        predictions.show(false);


        //predictions.show(false);

        double[] coef = lsvcModel.coefficients().toArray();
        List<Row> co = new ArrayList<>();
        for (double v1: coef){
            co.add(RowFactory.create(v1));
        }

        List<Row> row = Arrays.asList(
                RowFactory.create(lsvcModel.intercept()));

        StructType sch = new StructType(new StructField[]{
                new StructField("Coefficients", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult = spark.createDataFrame(co, sch);

        StructType sch2 = new StructType(new StructField[]{
                new StructField("Intercept", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult2 = spark.createDataFrame(row, sch2);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.predictions
                //.coalesce(1)
                .write()
                .format("parquet")
                //.format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));
/*
        this.dfResult
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.dfResult2
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));
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
        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s, %s as label FROM dft", processedLabelNames, lbl));
        this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
        names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        //df.show();
    }
}