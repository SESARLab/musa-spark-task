package it.unimi.evotion.tasks.dtcPredict;

import it.unimi.evotion.tasks.dtcPredict.Task;
import it.unimi.evotion.tasks.dtcPredict.dtRecord;
import it.unimi.evotion.tasks.dtcPredict.dtTreeStruct;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.SparseVector;
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

public class DTCPredictTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private Dataset<Row> dfTree;
    private SparkSession spark;
    /*
    private String csvData;
    private String labelName;
    private String label;
    private String treePath;
    private int    labelIndex;
    private String resultPath;
    */
    private String[] allNames;
    private List<String> lcn = new ArrayList<>();
    private double testError;
    //private String modelData;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Dataset<Row> predictions;
    private Dataset<Row> predResults;
    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the Decision Tree Classification Predict
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

        this.logger = LogManager.getLogger(DTCPredictTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 5)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.modelData  = (String) args[1];
        this.labelName = (String) args[2];
        this.label = (String) args[3];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
        this.treePath = ((String) args[4]) + ".tree";
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("DTCPredictTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("DTCPredictTask")
                    .getOrCreate();
        }
    }
    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        // TODO: Change the parameters it is not <<test>> should be csvData
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
        //df2.show(false);
        df2.createOrReplaceTempView("v12");



        formatData();
        // Make predictions.
        PipelineModel model = PipelineModel.load(parameters.get("modelData"));
        predictions = model
                .transform(this.df);

        predResults = predictions
                .drop("indexedFeatures")
                .drop("features")
                //.drop("indexedLabel")
                //.drop("predictedLabel")
                ;

        VectorDisassembler vd = new VectorDisassembler().setInputCol("rawPrediction");
        predResults = vd.transform(predResults).drop("rawPrediction");
        VectorDisassembler vd2 = new VectorDisassembler().setInputCol("probability");
        predResults = vd2.transform(predResults).drop("probability");
        predResults.show(false);

        //C:\Users\narda\Documents\dropouts_262.csv

        //C:\Users\narda\Documents\splitDropouts262\training

        // Select example rows to display.

        // Select (prediction, true label) and compute test error.
 /*       MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double accuracy = evaluator.evaluate(predictions);
        DecisionTreeClassificationModel treeModel = (DecisionTreeClassificationModel)(model.stages()[2]);
        this.featureImportances = treeModel.featureImportances();
        this.debugString = treeModel.toDebugString();
        this.testError = 1 - accuracy;
        */
        //createResults();




        String[] predCols = predResults.columns();
        List<String> prCols = Arrays.asList(predCols);
        String snames = String.join(",", prCols);

/*
        List<String> lnames = Arrays.asList(names);
        String snames = String.join(",", lnames);*/
        predResults.createOrReplaceTempView("v21");
        //dfResult = spark.sql(String.format("SELECT row_number() over (order by %s DESC) as ID, %s FROM v21", snames, snames));
        predResults = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM v21", snames));
        //predResults = predResults.drop("ID");
        predResults.createOrReplaceTempView("v22");
        predResults.show(false);
        //predResults = spark.sql("SELECT AVG_HA_USAGE,VARIANCE_HA_USAGE,HEARING_LOSS_SEVERITY,AGE,label,indexedLabel,predictedLabel FROM v22 WHERE predictedLabel=0 OR predictedLabel=1");
        //predResults = spark.sql("SELECT v12.PATIENT_ID, v22.predictedLabel FROM v12,v22 WHERE v12.ID=v22.ID");//.drop("ID");
        predResults = spark.sql("SELECT * FROM v12, v22 WHERE v12.ID=v22.ID").drop("ID");

        //predResults.show(false);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.predResults
                //.coalesce(1)
                .write()
                .format("parquet")
                //.format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));
/*
        this.dfResult2
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.predResults
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));
*/
//        this.dfTree.write().mode(SaveMode.Append).text(parameters.get("resultPath"));
        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    private void createResults(){
        int[] indices = ((SparseVector) featureImportances).indices();
        double[] values = ((SparseVector) featureImportances).values();
        List<dtRecord> lfm = new ArrayList<>();

        StructType schema = this.df.schema();
        allNames = schema.fieldNames();

        for(int i=0; i<indices.length; ++i) {
            String name = allNames[indices[i]];
            double value = values[i];
            lfm.add(new dtRecord(name, value));
        }

        List<dtTreeStruct> tree = new ArrayList<>();
        tree.add(new dtTreeStruct(debugString));

        List<Row> rows = Arrays.asList(
                RowFactory.create(testError));

        StructType sch = new StructType(new StructField[]{
                new StructField("Test_Error", DataTypes.DoubleType, true, Metadata.empty()),
        });

        dfResult = spark.createDataFrame(lfm, dtRecord.class);
        dfTree = spark.createDataFrame(tree, dtTreeStruct.class);
        dfResult2 = spark.createDataFrame(rows, sch);
    }

    void formatData(){
/*
        StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(parameters.get("label"))))
                .setHandleInvalid("keep")
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
