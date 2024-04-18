package it.unimi.evotion.tasks.nbayes_model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class NBayesModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;

    /*
    private String train;
    private String labelName;
    private String label;
    private int labelIndex;
    private String resultPath;
    */
    private NaiveBayesModel model;
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private Vector featureImportances;
    private String debugString;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    /**
     * Create the Naive Bayes Model
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

        this.logger = LogManager.getLogger(NBayesModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 4)
            throw new InvalidParameterException("Missing parameters.");

        this.train = (String) args[0];
        this.labelName = (String) args[1];
        this.label = (String) args[2];
        this.labelIndex = -1;
        this.resultPath = (String) args[3];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("nBayesTask")
                    .getOrCreate();
        } catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("nBayesTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                .format("csv") //was parquet
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("training"));

        formatData();
        NaiveBayes nb = new NaiveBayes();
// train the model
        this.model = nb.fit(this.df);
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.model.write().overwrite().save(parameters.get("resultPath"));
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
