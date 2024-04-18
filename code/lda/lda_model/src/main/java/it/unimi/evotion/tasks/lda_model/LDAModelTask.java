package it.unimi.evotion.tasks.lda_model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class LDAModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
   // private String csvData;
   // private String labelName;
    //private String label;
   // private int    labelIndex;
   // private String k;
   // private String resultPath;
    private List<String> lcn = new ArrayList<>();
    private Dataset<Row> df;    // input dataframe
    private LDAModel model;
    private LDA lda;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Vector featureImportances;
    private String debugString;
    private static final int maxIter=20;
    private static final String optimizer="em";


    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the LDA Model
     *
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] user-defined features used for building the model
     *      args[2] k number of clusters
     *      args[3] maximum number of ityerations
     *      args[4] number of topics
     *      args[5] url_json|url_csv result of lda information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(LDAModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 5)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.k  = (String) args[2];
        this.maxIter = (String) args[3];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LDAModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LDAModelTask")
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
        // Trains a LDA model.

        this.lda = new LDA()
                .setK(Integer.parseInt(parameters.get("k")))
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter))))
                .setOptimizer(parameters.getOrDefault("optimizer", optimizer)) //Optimizer or inference algorithm used to estimate the LDA model. Currently
        // supported (case-insensitive): - "online": Online Variational Bayes (default) - "em": Expectation-Maximization
                //.setKeepLastCheckpoint()
        ;
        this.model = lda.fit(df);
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        //this.lda.write().overwrite().save(resultPath);
        this.model.write().overwrite().save(parameters.get("resultPath"));
        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData(){

        String processedLabelNames = this.parameters.get("labelName").replaceAll("'", "");
        for(String c : df.columns()) {
            df = df.withColumnRenamed(c, c.replaceAll("'", ""));
        }
        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT %s FROM dft", processedLabelNames));
        this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
        String[] names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
    }

}
