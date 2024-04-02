package it.unimi.evotion.tasks.lda_predict;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.DistributedLDAModel;
import org.apache.spark.ml.clustering.LocalLDAModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class LDAPredictTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private String csvData;
    private String labelName;
    private int    labelIndex;
    private Dataset<Row> transformed;
    private double ll;
    private double lp;
    private Dataset<Row> topics;
    private List<String> lcn = new ArrayList<>();
    private Optional<Dataset<Row>> df2;
    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Dataset<Row> dfResult2;
    private Dataset<Row> dfResult3;
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
     * Create the LDA Predict
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

        this.logger = LogManager.getLogger(LDAPredictTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));

        /*if (args.length < 5)
            throw new InvalidParameterException("Missing parameters.");

        this.csvData = (String) args[0];
        this.labelName = (String) args[2];
        this.modelData = (String) args[1];
        this.topics = (String) args[3];
        this.labelIndex = -1;
        this.resultPath = (String) args[4];
*/
        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("LDAPredictTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("LDAPredictTask")
                    .getOrCreate();
        }
    }
    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark.read()
                .format("csv")
                //.format("parquet")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        formatData();
        //LDA lda = LDA.load(modelData);
        //LDAModel model = lda.fit(df);

        File file = new File(parameters.get("modelData")+"/oldModel");
        if(file.exists()){
            DistributedLDAModel model = DistributedLDAModel.load(parameters.get("modelData"));
            ll = model.logLikelihood(df);
            lp = model.logPerplexity(df);
            topics = model.describeTopics(Integer.parseInt(parameters.get("topics")));
            transformed = model.transform(df);

        }
        else{
            LocalLDAModel model = LocalLDAModel.load(parameters.get("modelData"));
            ll = model.logLikelihood(df);
            lp = model.logPerplexity(df);
            topics = model.describeTopics(Integer.parseInt(parameters.get("topics")));
            transformed = model.transform(df);

        }


       //LocalLDAModel model = LocalLDAModel.load(parameters.get("modelData"));

        //DistributedLDAModel model = DistributedLDAModel.load(parameters.get("modelData"));
/*
        ll = model.logLikelihood(df);
        lp = model.logPerplexity(df);

        topics = model.describeTopics(Integer.parseInt(parameters.get("topics")));
*/
        List<String> columns = Arrays.stream(parameters.get("labelName").split(",")).collect(Collectors.toList());
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
        //
        //
        // transformed = model.transform(df);
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

        this.dfResult
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.dfResult2
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

        this.dfResult3
                .coalesce(1)
                .write()
                //.format("parquet")
                .format("csv")
                .mode(SaveMode.Append)
                .option("header", "true")
                .save(parameters.get("resultPath"));

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
        //df.show();
    }

}
