package it.unimi.evotion.tasks.bkmeansModel;

import it.unimi.evotion.tasks.bkmeansModel.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class BKMeansModelTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
   /*
    private String csvData;
    private int    labelIndex;
    private String k;
    private String maxIter;
    private String MinDivisibleClusterSize;
    private String resultPath;
    */
    private List<String> lcn = new ArrayList<>();
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Dataset<Row> df;    // input dataframe
    private BisectingKMeansModel model;
    private String debugString;

    private static final int maxIter = 5;
    private static final double MinDivisibleClusterSize = 1;


    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the Bisecting KMeans Model
     *
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] k number of clusters
     *      args[2]  path for saving the model
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(BKMeansModelTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 5)
            throw new InvalidParameterException("Missing parameters.");
        this.csvData = (String) args[0];
        this.k  = (String) args[1];
        this.maxIter = (String) args[2];
        this.MinDivisibleClusterSize = (String) args[3];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("BKMeansModelTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("BKMeansModelTask")
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
                .load(parameters.get("csvData"));

        formatData();
        BisectingKMeans bkm = new BisectingKMeans()
                .setK(Integer.parseInt(parameters.get("k")))   //5
                .setMaxIter(Integer.parseInt(parameters.getOrDefault("maxIter", String.valueOf(maxIter))))  //20
                .setMinDivisibleClusterSize(Double.parseDouble(parameters.getOrDefault("MinDivisibleClusterSize", String.valueOf(MinDivisibleClusterSize))))   //1
                .setSeed(1);
        this.model = bkm.fit(df);
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
