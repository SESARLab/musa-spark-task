package it.unimi.evotion.tasks.gmmPredict;

import it.unimi.evotion.tasks.gmmPredict.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class gmmpTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    /*
    private String modelData;
    private String csvData;
    private int    labelIndex;
    private String resultPath;
    */
    private List<String> lcn = new ArrayList<>();

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
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
     * Create the KMeans Predictions
     *
     *
     * @param args
     *      args[0] csv path
     *      args[1] model path
     *      args[2] url_json|url_csv result of kmeans information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(gmmpTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("gmmpTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("gmmpTask")
                    .getOrCreate();
        }
    }
    @Override
    public void run(Object... params) throws Exception {

        //load the data
        // load the dataset on the file
        this.df = spark.read()
                .format("csv") // was parquet
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        // load the model
        formatData();
        GaussianMixtureModel model = GaussianMixtureModel.load(parameters.get("modelData"));
// Make predictions
        Dataset<Row> predictions = model.transform(df);
        dfResult = predictions.drop("features");
        dfResult.show();
        //double cost = model.computeCost(df);



// Shows the result.
    /*    Vector[] centers = model.clusterCenters();
        List<Vector> lcenters = Arrays.asList(centers);

        ArrayList<Vector> als2 = new ArrayList<>();
        for (int i = 0; i < lcenters.size(); i++){
            als2.add(lcenters.get(i));
        }

        ArrayList<Row> rows22 = new ArrayList<>();

        for (int j = 0; j < als2.size(); j++){
            rows22.add(RowFactory.create(als2.get(j)));
        }

        StructType sch22 = new StructType(new StructField[]{
                new StructField("Cluster_Center", new VectorUDT(), true, Metadata.empty())
        });
*/
        //dfResult = spark.createDataFrame(rows22, sch22);
        VectorDisassembler vd = new VectorDisassembler().setInputCol("probability");
        dfResult = vd.transform(dfResult).drop("probability");
        dfResult.show();
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));

    /*    this.dfResult2.coalesce(1).write()
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

        df.createOrReplaceTempView("dft");
        df = spark.sql(String.format("SELECT * FROM dft"));

        String[] names = df.columns();
        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
    }
}
