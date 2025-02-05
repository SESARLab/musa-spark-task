package it.unimi.evotion.tasks;

import it.unimi.evotion.tasks.anova.AnovaResult;
import it.unimi.evotion.tasks.anova.MultiWayAnova;
import it.unimi.evotion.tasks.anova.OneWayAnova;
import it.unimi.evotion.tasks.anova.TwoWayAnova;
import it.unimi.evotion.tasks.util.AnovaRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class AnovaTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------

    private static int MAX_FEATURES_SUPPORTED = 8;

    private Logger logger;
    private SparkSession spark;
    //private String csvData;
    protected String labelName;
    protected int    labelIndex;
    //private String resultPath;
    private String[] featureNames;
    private Dataset<Row> df1Map;

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    protected Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private AnovaResult result;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    /**
     * Compute the ANOVA (Analysis of Variance)
     *
     * with 2 columns, it is used the OneWay ANOVA
     * with 3 columns, it is used the TwoWay ANOVA
     * with 4 or more columns, it is used the MultiWay ANOVA
     *
     * @param args
     *      args[0] url_csv input dataset (in CSV with header format)
     *      args[1] name|index (0-based) of the dependent (label) column/variable
     *      args[2] url_json result of ANOVA (in JSON format)
     * @throws Exception
     */
    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(AnovaTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));
/*
        if (args.length < 3)
            throw new InvalidParameterException("Missing parameters (url_csv, name|index, url_result)");

        this.csvData = (String) args[0];
        this.labelName = (String) args[1];
        this.labelIndex = -1;
        this.resultPath = (String) args[2];
        this.labelName = parameters.get("labelName");
*/

        this.labelIndex = -1;


        try {
            this.spark = SparkSession
                .builder()
                 //.master("local[2]")
                // .master("yarn")
                .appName("Anova")
                .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                .builder()
                .master("local[4]")
                .config("spark.master", "local")
                .config("spark.driver.bindAddress", "127.0.0.1")
                // .master("yarn")
                .appName("Anova")
                .getOrCreate();

            this.labelName= parameters.get("labelName");
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        // load the dataset on the file
        this.df = spark
                .read()
                .format("csv")
            //.format("parquet")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(parameters.get("csvData"));

        this.logger.info(String.format("SCHEMA: %s ",df.columns().toString()));
        System.out.println(this.labelName);
        selectFeaturesAndLabel();
        convertFeaturesAndLabel();
        // computeCountsAndSums();

        IAnova anova;


        this.df.show();

        switch (this.featureNames.length) {
            case 0:
                throw new InvalidParameterException(String.format("Dataset %s has no features", parameters.get("csvData")));
            case 1:
                anova = new OneWayAnova(this.df, parameters.get("labelName"), this.featureNames);
                break;
            case 2:
                anova = new TwoWayAnova(this.df, parameters.get("labelName"), this.featureNames);
                break;
            default:
                anova = new MultiWayAnova(this.df, parameters.get("labelName"), this.featureNames);
        }

        anova.init();
        anova.evaluate();
        result = anova.result();
    }

    @Override
    public void postProcessing(Object... params) throws Exception {
        /*
        if (parameters.get("resultPath").endsWith(".json")) {
            ObjectMapper jser = new ObjectMapper();
            jser.enable(SerializationFeature.INDENT_OUTPUT);
            jser.writeValue(new File(parameters.get("resultPath")), result);
        }
        */

        // if (parameters.get("resultPath").endsWith(".csv")) {
            convertResult();
            df1Map = dfResult.select(functions.concat(dfResult.col("category"), functions.lit("_"), dfResult.col("name")).as("Paramnames"), dfResult.col("value").as("Values"));
            this.df1Map
                    //.coalesce(1)
                    .write()
                    .format("parquet")
                    //.format("csv")
                    .mode(SaveMode.Overwrite)
                    .option("header", "true")
                    .save(parameters.get("resultPath"));
       // }
        /*
        this.dfResult.write()
                .format("csv")
                .mode(SaveMode.Overwrite)
                .option("header", "true")
                .save(parameters.get("resultPath"));
        */
        this.spark.stop();
        this.spark.close();

    }

    private void convertResult() {

        AnovaResult r = result;

        // List<Row> rlist = new ArrayList<>();
        List<AnovaRecord> rlist = new ArrayList<>();

        // public Map<String, Long> degreeOfFreedom = new TreeMap<>(COMPARATOR);
        for(String key : r.degreeOfFreedom.keySet())
            rlist.add(AnovaRecord.create("degreeOfFreedom", key, r.degreeOfFreedom.get(key)));

        // public Map<String, Double> sumOfSquared = new TreeMap<>(COMPARATOR);
        for(String key : r.sumOfSquared.keySet())
            rlist.add(AnovaRecord.create("sumOfSquared", key, r.sumOfSquared.get(key)));

        // public Map<String, Double> meanSquared = new TreeMap<>(COMPARATOR);
        for(String key : r.meanSquared.keySet())
            rlist.add(AnovaRecord.create("meanSquared", key, r.meanSquared.get(key)));

        // public Map<String, Double> fRatio = new TreeMap<>(COMPARATOR);
        for(String key : r.fRatio.keySet())
            rlist.add(AnovaRecord.create("fRatio", key, r.fRatio.get(key)));

        // public Map<String, Double> pValue = new TreeMap<>(COMPARATOR);
        for(String key : r.pValue.keySet())
            rlist.add(AnovaRecord.create("pValue", key, r.pValue.get(key)));

        // public Map<String, Long> cellCounts = new TreeMap<>(COMPARATOR);
        for(String key : r.cellCounts.keySet())
            rlist.add(AnovaRecord.create("cellCounts", key, r.cellCounts.get(key)));

        // public Map<String, Double> cellSums = new TreeMap<>(COMPARATOR);
        for(String key : r.cellSums.keySet())
            rlist.add(AnovaRecord.create("cellSums", key, r.cellSums.get(key)));

        // public Map<String, Double> cellMeans = new TreeMap<>(COMPARATOR);
        for(String key : r.cellMeans.keySet())
            rlist.add(AnovaRecord.create("cellMeans", key, r.cellMeans.get(key)));

        dfResult = spark.createDataFrame(rlist, AnovaRecord.class);
        dfResult.show();

        // dfResult.describe();
        // dfResult.show();
    }

    // ----------------------------------------------------------------------
    // Implementation
    // ----------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void selectFeaturesAndLabel() {
        this.logger.info(String.format("Label before the selection: %s (%d)", parameters.get("labelName"), this.labelIndex));
        StructType schema = this.df.schema();
        String[] fnames = schema.fieldNames();

        // label
        {
            // check if the label is specified as index or name
            // It is possibile to use numbers < 0 to count from the end

           /*
            try {
                this.labelIndex = Integer.MAX_VALUE;
                this.labelIndex = Integer.parseInt(this.labelName);
            } catch (Exception e) { }

            if (this.labelIndex != Integer.MAX_VALUE)
                // convert the column index in the column name
                try {
                    if (this.labelIndex < 0)
                        this.labelIndex += fnames.length;
                    this.labelName = schema.fieldNames()[this.labelIndex];
                }
                catch (ArrayIndexOutOfBoundsException e) {
                    throw new IllegalArgumentException(String.format("Invalid column index: select %d, valid range [0,%d]",
                        this.labelIndex, schema.fieldNames().length-1));
                }
            else
                this.labelIndex = schema.fieldIndex(parameters.get("labelName"));
            */
            this.labelIndex = schema.fieldIndex(parameters.get("labelName"));
            this.logger.info(String.format("Label: %s (%d)", parameters.get("labelName"), this.labelIndex));
        }

        // features
        {
            int n = fnames.length-1;
            this.featureNames = new String[n];

            for (int i=0, j=0; i<fnames.length; ++i) {
                if (i == this.labelIndex) continue;

                this.featureNames[j] = fnames[i];

                ++j;
            }
        }

        // check the number of features
        // Because it is necessary to analyze the powerset of the features,
        // it is necessary to process 2^n subsets, where n is the number of fetaures.
        //
        // MAX_FEATURES_SUPPORTED define the max number of features, and
        // 2^MAX_FEATURES_SUPPORTED the max number of subsets to process
        //
        {
            if (this.featureNames.length > MAX_FEATURES_SUPPORTED)
                throw new IllegalArgumentException(String.format("Unsupported ANOVA with more of %d features: actual %d",
                    MAX_FEATURES_SUPPORTED,
                    this.featureNames.length));
        }
    }

    /**
     * Convert the columns of the dataset such that:
     * - the column "label" is numeric (double)
     * - the feature columns are categoricla (S"string")
     */
    private void convertFeaturesAndLabel() {
        String label = parameters.get("labelName");
        this.df = this.df.withColumn(label, this.df.col(label).cast(DataTypes.DoubleType));

        for(String feature : this.featureNames)
            this.df = this.df.withColumn(feature, this.df.col(feature).cast(DataTypes.StringType));
    }

    // ----------------------------------------------------------------------
    // End
    // ----------------------------------------------------------------------

}
