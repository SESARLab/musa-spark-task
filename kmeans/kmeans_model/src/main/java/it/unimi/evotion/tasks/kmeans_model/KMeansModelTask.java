package it.unimi.evotion.tasks.kmeans_model;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class KMeansModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    /*
    private String csvData;
    private int    labelIndex;
    private String k;
    private String resultPath;
    */
    private static final int maxIter=20;
    private static final String initMode="k-means||";
    private static final int initSteps=2;
    private static final double epsilon=0.000001;
    private List<String> lcn = new ArrayList<>();

    private Dataset<Row> df;    // input dataframe
    private KMeansModel model;
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
     * Create the KMeans Model
     *
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] number of clusters k
     *      args[2] path for saving the model
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(KMeansModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 7)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.k  = (String) args[1];
        this.maxIter = (String) args[2];
        this.initSteps  = (String) args[3];
        this.initMode  = (String) args[4];
        this.epsilon  = (String) args[5];
        this.labelIndex = -1;
        this.resultPath = (String) args[6];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("KMeansModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("KMeansModelTask")
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
                .load(parameters.get("csvData"));

        formatData();
        KMeans kmeans = new KMeans()
                .setK(Integer.parseInt(parameters.get("k")))   //5
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter))))   //20
                .setInitSteps(Integer.parseInt(parameters.getOrDefault("initSteps", String.valueOf(initSteps))))   //2
                .setInitMode(parameters.getOrDefault("initMode", initMode))  //k-means||
                .setTol(Double.parseDouble(parameters.getOrDefault("epsilon", String.valueOf(epsilon))))    //0.0001
                .setSeed(1L);
        this.model = kmeans.fit(df);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.model.write().overwrite().save(parameters.get("resultPath"));
        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData(){

        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT * FROM dft"));
        String[] names = df.columns();
        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
    }

}
