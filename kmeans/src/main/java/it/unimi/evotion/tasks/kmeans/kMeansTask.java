package it.unimi.evotion.tasks.kmeans;

//import it.unimi.evotion.tasks.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
//import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class kMeansTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private String csvData;
    private String labelName;
    //private String label;
    private int    labelIndex;
    private String k;
    private String resultPath;
    private String[] allNames;
    //private String[] featureNames;
    private List<String> lcn = new ArrayList<>();

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    //private Dataset<Row> dfResult3;


    private Vector featureImportances;
    private String debugString;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * Create the KMeans Model
     *
     * Note: if the label is numeric (float|double) the decision tree is used
     * for regressione, otherwise (int) for classification
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] number of clusters k
     *      args[2] url_json|url_csv result of kmeans information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(kMeansTask.class);

        if (args.length < 3)
            throw new InvalidParameterException("Missing parameters.\n" +
                    "Usage: kMeansTask <input_csv_data> <number_of_clusters> <result_path>");

        this.csvData = (String) args[0];
    //    this.labelName = (String) args[1];
        this.k  = (String) args[1];
        this.labelIndex = -1;
        this.resultPath = (String) args[2];

        try {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    // .master("yarn")
                    .appName("kMeansTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    // .master("yarn")
                    .appName("kMeansTask")
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
                .load(csvData);

        convertFeatures();
        formatData();
        // Trains a k-means model.
        KMeans kmeans = new KMeans().setK(Integer.parseInt(this.k)).setSeed(1L);
        KMeansModel model = kmeans.fit(df);

// Make predictions
        Dataset<Row> predictions = model.transform(df);
        dfResult = predictions.drop("features");
        dfResult.show();
// Evaluate clustering by computing Silhouette score
 //       ClusteringEvaluator evaluator = new ClusteringEvaluator();
 //       double silhouette = evaluator.evaluate(predictions);
        //System.out.println("Silhouette with squared euclidean distance = " + silhouette);


        double cost = model.computeCost(df);
        //System.out.println("Within Set Sum of Squared Errors = " + cost);

// Shows the result.
        Vector[] centers = model.clusterCenters();

        List<Vector> lcenters = Arrays.asList(centers);
        //System.out.println(Arrays.asList(centers));
        //System.out.println("Cluster Centers: ");
       // for (Vector center: centers) {
        //    System.out.println(center);
       // }


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

        dfResult2 = spark.createDataFrame(rows22, sch22);
        VectorDisassembler vd3 = new VectorDisassembler().setInputCol("Cluster_Center");
        dfResult2 = vd3.transform(dfResult2).drop("Cluster_Center");
        dfResult2.show();


/*
        ArrayList<String> als = new ArrayList<>();
        for (int i = 0; i < lcenters.size(); i++){
            als.add(lcenters.get(i).toString());
        }

        ArrayList<Row> rows2 = new ArrayList<>();

        for (int j = 0; j < als.size(); j++){
            rows2.add(RowFactory.create(als.get(j)));
        }

        //System.out.println("something " + als);

        StructType sch2 = new StructType(new StructField[]{
                new StructField("Cluster_Centers", DataTypes.StringType, true, Metadata.empty()),
        });

        dfResult3 = spark.createDataFrame(rows2, sch2);
        dfResult3.show();

     //   System.out.println("Within Set Sum of Squared Errors = " + cost);

        List<Row> rows = Arrays.asList(
                RowFactory.create(silhouette, cost));

        StructType sch;
        sch = new StructType(new StructField[]{
                new StructField("Silhouette", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Within_Sum_Squared_Error", DataTypes.DoubleType, true, Metadata.empty())
        });

        dfResult2 = spark.createDataFrame(rows, sch); */

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(resultPath);

   /*     this.dfResult2.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);
*/
        this.dfResult2.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData(){

     /*   String processedLabelNames = this.labelName.replaceAll("'", "");
         for(String c : df.columns()) {
            df = df.withColumnRenamed(c, c.replaceAll("'", ""));
        }
*/
        df.createOrReplaceTempView("dft");
        //df = spark.sql(String.format("SELECT %s FROM dft", processedLabelNames));
        df = spark.sql(String.format("SELECT * FROM dft"));


        String[] names = df.columns();

     //   this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
     //   String[] names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        //df.show();
    }

    private void convertFeatures() {
        List<String> stringFeatures = new ArrayList<>();

        for(StructField field : this.df.schema().fields()) {
            if (field.dataType().equals(DataTypes.StringType))
                stringFeatures.add(field.name());
        }

        for(String sf : stringFeatures) {
            String rn = "str-"+sf;
            this.df = this.df.withColumnRenamed(sf, rn);
            StringIndexer encoder = new StringIndexer()
                    .setInputCol(rn)
                    .setOutputCol(sf);

            this.df = encoder.fit(this.df).transform(this.df).drop(rn);
            this.df = this.df.withColumn(sf, this.df.col(sf).cast(DataTypes.IntegerType) );
        }
        this.df.printSchema();
        this.df.show();
    }

}
