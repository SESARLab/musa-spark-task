package it.unimi.evotion.tasks;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.param.DoubleParam;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class mmsTask implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private List<String> lcn = new ArrayList<>();

    public static final double min = 0;
    public static final double max = 1;

    private Dataset<Row> df;    // input dataframe
    private Dataset<Row> dfResult;
    private Map<String,String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Vector featureImportances;
    private String debugString;
    private String[] names;
    private Dataset<Row> df1;
    //private List<String> ncl;

    // ----------------------------------------------------------------------
    // Interface implementation
    // ----------------------------------------------------------------------

    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(mmsTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("mmsTask")
                    .getOrCreate();
        }
        catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.master", "local")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    // .master("yarn")
                    .appName("mmsTask")
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


        //df.show(false);
        df.createOrReplaceTempView("t");
        df = spark.sql(String.format("SELECT * FROM t ORDER BY %s", parameters.get("constParams")));

        df.createOrReplaceTempView("v11");
        //Dataset<Row> df2 = spark.sql(String.format("SELECT row_number() over (order by %s DESC) as ID, %s FROM v11", parameters.get("constParams"), parameters.get("constParams")));
        Dataset<Row> df2 = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM v11", parameters.get("constParams"), parameters.get("constParams")));
        df2.show(false);
        df2.createOrReplaceTempView("v12");

        formatData();


/*
        String strnames = parameters.get("labelName");
        String namess[] = strnames.split(",");
        String[] dfcols = df.columns();
        List<String> al = Arrays.asList(namess);
        List<String> dif1 = new ArrayList<>(CollectionUtils.subtract(Arrays.asList(dfcols), al));
        String diff = String.join(",", dif1);
        System.out.println("DIFFERENT COLUMNS " + diff);
        df1 = spark.sql(String.format("SELECT * FROM t ORDER BY %s", diff));
        df1.createOrReplaceTempView("v11");
        Dataset<Row> df2 = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM v11", diff, diff));
        df2.show(false);
        df2.createOrReplaceTempView("v12");
        
        formatData();
*/



        


        MinMaxScaler scaler = new MinMaxScaler()
                .setInputCol("features")
                .setOutputCol("scaledFeatures")
                .setMin(Double.parseDouble(parameters.getOrDefault("min", String.valueOf(min))))
                .setMax(Double.parseDouble(parameters.getOrDefault("max", String.valueOf(max))));

        MinMaxScalerModel scalerModel = scaler.fit(df);
        Dataset<Row> results = scalerModel.transform(df);

        VectorDisassembler vd = new VectorDisassembler().setInputCol("scaledFeatures");
        dfResult = vd.transform(results).drop("features").drop("scaledFeatures");

        for (String s: names){
            dfResult = dfResult.drop(s);
        }

        String[] c = dfResult.columns();

            for (int i=0; i<c.length; i++) {
                String nc = c[i].replaceFirst("scaledFeatures(.*)", names[i]);
                dfResult = dfResult.withColumnRenamed(c[i], nc);
            }




       // String snames = String.join(",", al);


        List<String> lnames = Arrays.asList(names);
        String snames = String.join(",", lnames);
        dfResult.createOrReplaceTempView("v21");
        //dfResult = spark.sql(String.format("SELECT row_number() over (order by %s DESC) as ID, %s FROM v21", snames, snames));
        dfResult = spark.sql(String.format("SELECT monotonically_increasing_id() as ID, %s FROM v21", snames));
        dfResult.show(false);
        dfResult.createOrReplaceTempView("v22");
        dfResult = spark.sql("SELECT * FROM v12, v22 WHERE v12.ID=v22.ID").drop("ID");
        dfResult.show(false);

    }

    @Override
    public void postProcessing(Object... params) throws Exception {

        this.dfResult
                //.coalesce(1)
                .write()
                .format("csv")
                //.format("parquet")
                .mode(SaveMode.Overwrite)
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

        names = this.lcn.stream().toArray(String[]::new);
        VectorAssembler assembler1 = new VectorAssembler()
                .setInputCols(names)
                .setOutputCol("features");

        df = assembler1.transform(df);
        //df.show();
    }

}
