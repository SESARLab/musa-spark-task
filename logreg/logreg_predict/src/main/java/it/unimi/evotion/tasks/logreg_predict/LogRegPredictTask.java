package it.unimi.evotion.tasks.logreg_predict;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import java.util.stream.Collectors;

public class LogRegPredictTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> predictions;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;

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

        this.logger = LogManager.getLogger(LogRegPredictTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
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
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LogRegPredictTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LogRegPredictTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {
        // TODO: Port the classes in the correct package
        // load the dataset on the file
        this.df = spark.read()
                //.format("csv")
                .format("csv") // was parquet
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

        LogisticRegressionModel lrModel = LogisticRegressionModel.load(parameters.get("modelData"));
        predictions = lrModel.transform(this.df);
        predictions = predictions.drop("features");
        VectorDisassembler vd = new VectorDisassembler().setInputCol("rawPrediction");
        predictions = vd.transform(predictions).drop("rawPrediction");
        VectorDisassembler vd2 = new VectorDisassembler().setInputCol("probability");
        predictions = vd2.transform(predictions).drop("probability");
        predictions.show(false);


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

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

            this.predictions
                    //.coalesce(1)
                    .write()
                    //.format("csv")
                    .format("parquet")
                    .mode(SaveMode.Overwrite)
                    .option("header", "true")
                    .save(parameters.get("resultPath"));

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