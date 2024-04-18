package it.unimi.evotion.tasks.nbayes_predict;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
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

public class NBayesPredictTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    /*
    private String csvData;
    private String labelName;
    private String label;
    private int labelIndex;
    private String resultPath;
    private String modelData;
    */
    private List<String> lcn = new ArrayList<>();

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
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
     * Create the Naive Bayes Prediction
     * <p>
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args args[0] url_csv input dataset (in CSV with header format)
     *             args[1] user-defined features used for building the model
     *             args[2] name|index (0-based) of the dependent (label) column/variable
     *             args[3] training set size
     *             args[4] test set size
     *             args[5] url_json|url_csv result of naive bayes information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(NBayesPredictTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
     /*
        if (args.length < 5)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.modelData  = (String) args[1];
        this.labelName = (String) args[2];
        this.label = (String) args[3];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("NBayesPredictTask")
                    .getOrCreate();
        } catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("NBayesPredictTask")
                    .getOrCreate();
        }
    }


    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                .format("csv") // was parquet
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("test"));

        formatData();
        NaiveBayesModel model = NaiveBayesModel.load(parameters.get("modelData"));

        this.predictions = model.transform(this.df);
        predictions = predictions.drop("Features");

        VectorDisassembler vd = new VectorDisassembler().setInputCol("probability");
        predictions = vd.transform(predictions).drop("probability");
        VectorDisassembler vd2 = new VectorDisassembler().setInputCol("rawPrediction");
        predictions = vd2.transform(predictions).drop("rawPrediction");

        predictions.show(false);
// compute accuracy on the test set
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double accuracy = evaluator.evaluate(predictions);

        List<Row> row = Arrays.asList(
                RowFactory.create(accuracy));

        StructType sch = new StructType(new StructField[]{
                new StructField("Test_Set_Accuracy", DataTypes.DoubleType, true, Metadata.empty())});
        dfResult = spark.createDataFrame(row, sch);
        //System.out.println("Test set accuracy = " + accuracy);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.predictions.write()
                .format("parquet")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData() {
        StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(parameters.get("label"))))
                .setOutputCol("label");

        df = indexer.fit(df).transform(df);
        String processedLabelNames = parameters.get("labelName").replaceAll("'", "");
        for (String c : df.columns()) {
            df = df.withColumnRenamed(c, c.replaceAll("'", ""));
        }
        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s, label FROM dft", processedLabelNames));


        this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
        String[] names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        df.show();
    }

}
