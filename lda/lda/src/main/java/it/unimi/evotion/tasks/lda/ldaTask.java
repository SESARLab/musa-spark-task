package it.unimi.evotion.tasks.lda;

import it.unimi.evotion.tasks.lda.Task;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
//import org.apache.spark.ml.clustering.BisectingKMeans;
//import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
//import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import static org.apache.spark.sql.functions.*;

public class ldaTask implements Task {

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
    private String maxIter;
    private String topics;
    private String resultPath;
    private List<String> lcn = new ArrayList<>();
    private Optional<Dataset<Row>> df2;
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Dataset<Row> dfResult3;

    private Vector featureImportances;
    private String debugString;

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

        this.logger = LogManager.getLogger(ldaTask.class);

        if (args.length < 6)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.k  = (String) args[2];
        this.maxIter = (String) args[3];
        this.topics = (String) args[4];
        this.labelIndex = -1;
        this.resultPath = (String) args[5];

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("ldaTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("ldaTask")
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
        // Trains a LDA model.
        LDA lda = new LDA().setK(Integer.parseInt(this.k)).setMaxIter(Integer.parseInt(this.maxIter));
        LDAModel model = lda.fit(df);

        double ll = model.logLikelihood(df);
        double lp = model.logPerplexity(df);

        Dataset<Row> topics = model.describeTopics(Integer.parseInt(this.topics));

        List<String> columns = Arrays.stream(labelName.split(",")).collect(Collectors.toList());
        int cs = columns.size();


        List<Dataset<Row>> dsl = new ArrayList<>();

        for(int i=0; i<cs; i++){
            dsl.add(
                    topics.withColumn(String.format("Term_Indices_%s", i), topics.col("termIndices").getItem(i))
                            .withColumn(String.format("Term_Weights_%s", i), topics.col("termWeights").getItem(i))
            .drop("termIndices")
            .drop("termWeights"));
        }

        df2 = dsl.stream().reduce((x, y) -> {
            return x.join(y, "topic");
        });

        dfResult2 = df2.get();
        dfResult2.show();


        // Shows the result.
        Dataset<Row> transformed = model.transform(df);
        //transformed.show(false);

        VectorDisassembler vd3 = new VectorDisassembler().setInputCol("topicDistribution");
        dfResult3 = vd3.transform(transformed).drop("topicDistribution").drop("features");

        List<Row> rows = Arrays.asList(
                RowFactory.create(ll, lp));

        StructType sch;
        sch = new StructType(new StructField[]{
                new StructField("Log_Likelihood_Lower_Bound", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Perplexity_Upper_Bound", DataTypes.DoubleType, true, Metadata.empty())
        });
        dfResult = spark.createDataFrame(rows, sch);
    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(resultPath);

        this.dfResult2.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.dfResult3.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(resultPath);

        this.logger.info(this.debugString);
        this.spark.stop();
        this.spark.close();
    }

    void formatData(){

        String processedLabelNames = this.labelName.replaceAll("'", "");
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
            this.df = this.df.withColumn(sf, this.df.col(sf).cast(DataTypes.IntegerType));
        }
        this.df.printSchema();
        this.df.show();
    }

}
