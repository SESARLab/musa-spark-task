package it.unimi.evotion.tasks;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.KNNClassificationModel;
import org.apache.spark.ml.classification.KNNClassifier;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorDisassembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.KNNRegression;
import org.apache.spark.ml.regression.KNNRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class kNNCTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private List<String> lcn = new ArrayList<>();
    private KNNClassificationModel model;
    private Dataset<Row> df;    // input dataframe
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Vector featureImportances;
    private String debugString;
    private Dataset<Row> prediction;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(kNNCTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("kNNC")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    // .master("yarn")
                    .appName("kNNC")
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

        //convertFeatures();
        formatData();

        KNNClassifier knn = new KNNClassifier()
                .setK(Integer.parseInt(parameters.get("k")))
                .setTopTreeSize((int) (df.count() / Integer.parseInt(parameters.get("div"))));
                //.setBalanceThreshold(parameters.getOrDefault("sbt", String.valueOf(sbt)));

        this.model = knn.fit(df);
        prediction = this.model.transform(df);
        prediction = prediction.drop("features");

        VectorDisassembler vd = new VectorDisassembler().setInputCol("rawPrediction");
        prediction = vd.transform(prediction).drop("rawPrediction");
        VectorDisassembler vd2 = new VectorDisassembler().setInputCol("probability");
        prediction = vd2.transform(prediction).drop("probability");
        prediction.show();

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.prediction.coalesce(1).write()
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));
    }

    void formatData(){
        StringIndexer indexer = new StringIndexer()
                .setInputCol(String.valueOf(df.col(parameters.get("label"))))
                .setOutputCol("label");

        df = indexer.fit(df).transform(df);
        String processedLabelNames = parameters.get("labelName").replaceAll("'", "");
        for(String c : df.columns()) {
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
