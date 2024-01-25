package it.unimi.evotion.tasks;

import com.google.common.collect.Lists;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.DenseMatrix;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
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

public class pcaTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
   // private String csvData;
   // private String labelName;
    //private String label;
   // private int labelIndex;
   // private String k;
   // private String resultPath;
    private List<String> lcn = new ArrayList<>();

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Vector featureImportances;
    private String debugString;
    private Dataset<Row> results;
    private String[] names;
   // private SparkConf conf;
  //  private JavaSparkContext jsc;
   // private SQLContext sqc;
    private Dataset<Row> df2;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------
    /**
     * PCA
     *
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] user-defined features used for building the model
     *      args[2] k number of dimensions
     *      args[3] url_json|url_csv result of PCA information
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(pcaTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
        /*
        if (args.length < 4)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];   //HA_VOL,HA_PROG,PTS_TTS_RES
        this.k  = (String) args[2];  //2
        this.labelIndex = -1;
        this.resultPath = (String) args[3];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("pcaTask")
                    .getOrCreate();

     /*       this.conf = new SparkConf()
                    .setAppName("pcaTask");
                    //.setMaster("local[2]");
            this.jsc = new JavaSparkContext(spark.sparkContext());
            this.sqc = new SQLContext(jsc);*/

        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("pcaTask")
                    .getOrCreate();

        }

    }
    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                //.format("csv")
                .format("parquet")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));


        formatData();
        PCAModel pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(Integer.parseInt(parameters.get("k")))
                .fit(df);

        Dataset<Row> results = pca.transform(df).select("pcaFeatures");
        results.show(false);

        VectorDisassembler vd = new VectorDisassembler().setInputCol("pcaFeatures");
        dfResult = vd.transform(results).drop("pcaFeatures");
        System.out.println("Transformed: ");
        dfResult.show(false);

//-------------------------------WHAT IOANNIS WANTS------------------------------------------------------------------------------------------------

        //variances
        DenseVector vars = pca.explainedVariance();
        double[] avars = vars.toArray();

        int n = df.columns().length - 1; //number of features
        int k = Integer.parseInt(parameters.get("k"));  //number of principal components

        //list containing names of principal components
        List<String> pcs = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            pcs.add("PC_" + i);
        }


        DenseMatrix pcam = pca.pc();  //get prinicpal components
        //System.out.println(pcam);

        //create list of principal components with dimensions k x n
        double[] apca = pcam.transpose().toArray();
        List<Double> dpca = new ArrayList<>();
        for (double d : apca) {
            dpca.add(d);
        }
        //create list of principal components with dimensions n x k
        double[] apca2 = pcam.toArray();
        List<Double> dpca2 = new ArrayList<>();
        for (double d : apca2) {
            dpca2.add(d);
        }
        List<List<Double>> dlpca2 = Lists.partition(dpca, k);
        List<List<Double>> dlpca = Lists.partition(dpca, n);

        List<Row> rows = new ArrayList<>();
        //List<Double> rows2 = new ArrayList<>();

        for (int j = 0; j < pcs.size(); j++) {
            rows.add(RowFactory.create(pcs.get(j), avars[j]));
        }

        List<StructField[]> sch = new ArrayList<>();

        sch.add(new StructField[]{new StructField("PrincipalComponent", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Variance", DataTypes.DoubleType, true, Metadata.empty())});

        results = spark.createDataFrame(rows, new StructType(sch.get(0)));
        results.createOrReplaceTempView("t2");
        results = spark.sql("SELECT monotonically_increasing_id() as ID, PrincipalComponent, Variance FROM t2");
        results.createOrReplaceTempView("t22");
        results = spark.sql("SELECT row_number() over (order by ID ASC) as ID2, PrincipalComponent, Variance FROM t22");
        results.createOrReplaceTempView("t222");
        results.show(false);


//        for (int i = 0; i < names.length; i++){
//                sch2.add(new StructField[]{new StructField(names[i], DataTypes.DoubleType, true, Metadata.empty())});
//        }

        JavaSparkContext sparkContext1 = new JavaSparkContext(spark.sparkContext());

        List<Double[]> adlpca2 = new ArrayList<>();
        for(int i = 0; i < k; i++){
            adlpca2.add(dlpca.get(i).toArray(new Double[dlpca2.get(i).size()]));
        }

        JavaRDD<Row> rowRDD = sparkContext1.parallelize(adlpca2).map((Double[] row) -> RowFactory.create(row));


        List<StructField> lsf = new ArrayList<>();
        for (int i = 0; i < names.length; i++){
            lsf.add(new StructField(names[i], DataTypes.DoubleType, true, Metadata.empty()));
        }
        StructField[] lsfa = lsf.toArray(new StructField[lsf.size()]);
        StructType sch3 = DataTypes.createStructType(lsfa);
        df2 = spark.sqlContext().createDataFrame(rowRDD, sch3).toDF();
        df2.createOrReplaceTempView("t1");
        System.out.println("NAMEZZ " + parameters.get("labelName"));
        df2 = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM t1 ", parameters.get("labelName")));
        df2.createOrReplaceTempView("t11");
        df2 = spark.sql(String.format("SELECT row_number() over (order by ID ASC) as ID2, * FROM t11", parameters.get("labelNames")));
        df2 = df2.drop("ID");
        df2.createOrReplaceTempView("t111");
        df2 = spark.sql("SELECT t222.PrincipalComponent AS PrincipalComponent, t111.*, t222.Variance AS VARIANCE " +
                " FROM t111, t222" +
                " WHERE t111.ID2=t222.ID2").drop("ID2");
        df2.show(false);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult
                //.coalesce(1)
                .write()
                .format("parquet")
                //.format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.df2
                //.coalesce(1)
                .write()
                .format("parquet")
                //.format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath")+"/Coefficients");

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
        names = this.lcn.stream().toArray(String[]::new);

        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        //df.show();
    }

}
