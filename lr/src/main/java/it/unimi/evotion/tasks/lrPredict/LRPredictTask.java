package it.unimi.evotion.tasks.lrPredict;

import it.unimi.evotion.tasks.lrPredict.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class LRPredictTask implements Task {

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
    private String modelData;
    */
    private Dataset<Row> pred;
    private String str2;
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
     * Create the Linear Regression Predict
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

        this.logger = LogManager.getLogger(LRPredictTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 5)
            throw new InvalidParameterException("Missing parameters");

        this.csvData = (String) args[0];
        this.labelName = (String) args[2];
        this.label = (String) args[3];
        this.modelData = (String) args[1];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LRPredictTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LRPredictTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                //.format("parquet")
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        formatData();
        LinearRegressionModel model = LinearRegressionModel.load(parameters.get("modelData"));
        predictions = model.transform(this.df);
        predictions = predictions.drop("features");
        //predictions.show();
        predictions.createOrReplaceTempView("p");
        predictions.show(false);
        pred = spark.sql(String.format("SELECT label AS %s, %s, prediction" +
                " FROM p " +
                " ORDER BY label", parameters.get("label"), str2));
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.pred
                .coalesce(1)
                .write()
                .format("csv")
                //.format("parquet")
                .mode(SaveMode.Append)
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

        //Check if label is contained in labelName and remove it if yes
        String pln[] =processedLabelNames.split(",") ;
        List<String> lpln = new ArrayList<>();
        for (int i = 0; i < pln.length; i++){
            lpln.add(pln[i]);
        }
        if(lpln.contains(String.format("%s", parameters.get("label")))) {
            lpln.remove(String.format("%s", parameters.get("label")));
        }
        str2 = String.join(",", lpln );

        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s, %s as label FROM dft", str2, lbl));
        this.lcn = Arrays.stream(str2.split(",")).collect(Collectors.toList());
        String[] names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        //df.show();
    }

}