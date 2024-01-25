package it.unimi.evotion.tasks.rfcPredict;

import it.unimi.evotion.tasks.rfcPredict.Task;
import it.unimi.evotion.tasks.rfcPredict.rfRecord;
import it.unimi.evotion.tasks.rfcPredict.rfTreeStruct;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
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

// TODO: Use the correct package
public class RFCPredictTask implements Task {

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
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
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
     * Create the Random Forest Classification Predict
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

        this.logger = LogManager.getLogger(RFCPredictTask.class);
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
                    .appName("RFCPredictTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("RFCPredictTask")
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
                .load(parameters.get("test"));

        formatData();

        PipelineModel model = PipelineModel.load(parameters.get("modelData"));
        predictions = model.transform(this.df);

        // Select example rows to display.
        predictions = predictions.drop("features").drop("indexedFeatures");

        VectorDisassembler vd = new VectorDisassembler().setInputCol("rawPrediction");
        predictions = vd.transform(predictions).drop("rawPrediction");
        VectorDisassembler vd2 = new VectorDisassembler().setInputCol("probability");
        predictions = vd2.transform(predictions).drop("probability");

        predictions.show(false);
        // Select (prediction, true label) and compute test error
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);
        //System.out.println("Test Error = " + (1.0 - accuracy));

        RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);

        this.featureImportances = rfModel.featureImportances();
        this.debugString = rfModel.toDebugString();
        this.testError = 1 - accuracy;

        createResults();

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.dfResult2.write()
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.predictions.write()
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.dfTree.write().mode(SaveMode.Append).text(parameters.get("resultPath"));
        this.logger.info(this.debugString);

        this.spark.stop();
        this.spark.close();
    }

    private void createResults(){
        int[] indices = ((SparseVector) featureImportances).indices();
        double[] values = ((SparseVector) featureImportances).values();
        List<rfRecord> lfm = new ArrayList<>();

        StructType schema = this.df.schema();
        allNames = schema.fieldNames();

        for(int i=0; i<indices.length; ++i) {
            String name = allNames[indices[i]];
            double value = values[i];
            lfm.add(new rfRecord(name, value));
        }

        List<rfTreeStruct> tree = new ArrayList<>();
        // this.debugString = rfModel.toDebugString();
        tree.add(new rfTreeStruct(debugString));

        List<Row> rows = Arrays.asList(
                RowFactory.create(testError));

        StructType sch = new StructType(new StructField[]{
                new StructField("Test_Error", DataTypes.DoubleType, true, Metadata.empty()),
        });

        dfResult = spark.createDataFrame(lfm, rfRecord.class);
        dfTree = spark.createDataFrame(tree, rfTreeStruct.class);
        dfResult2 = spark.createDataFrame(rows, sch);
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
