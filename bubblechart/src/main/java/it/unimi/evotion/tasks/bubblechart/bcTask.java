package it.unimi.evotion.tasks.bubblechart;

import org.apache.hadoop.conf.Configured;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BubbleChart;
import org.knowm.xchart.BubbleChartBuilder;
import org.knowm.xchart.style.Styler;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class bcTask extends Configured implements Task {

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------
    private Logger logger;
    private SparkSession spark;
    private List<String> lcn = new ArrayList<>();
    private static final String title = "";
    private String processedLabelNames;
    private Dataset<Row> df;
    private Map<String, String> parameters;
    private static final int FIRST_OCCURRENCE = 2;
    private static final int KEY = 0;
    private static final int VALUE = 1;
    private Vector featureImportances;
    private String debugString;

    private static final Integer height = 600;
    private static final Integer width = 800;
    private static final String sname = "test";


    // private Configuration conf;
   // private FileSystem fs;

    @Override
    public void init(Object... args) throws Exception {

        this.logger = LogManager.getLogger(bcTask.class);
        parameters = Arrays.stream(args).map(x -> x.toString().split("=", FIRST_OCCURRENCE))
                .collect(Collectors.toMap(x -> x[KEY], x -> x[VALUE]));

       // this.conf = getConf();
       // this.fs = FileSystem.get(conf);

        try {
            this.spark = SparkSession
                    .builder()
                    //.master("local[2]")
                    // .master("yarn")
                    .appName("bcTask")
                    .getOrCreate();
        } catch (Exception e) {
            this.spark = SparkSession
                    .builder()
                    .master("local[2]")
                    .config("spark.driver.bindAddress", "127.0.0.1")
                    // .master("yarn")
                    .appName("bcTask")
                    .getOrCreate();
        }
    }

    @Override
    public void run(Object... params) throws Exception {

        this.df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(parameters.get("csvData"));

        String col1 = parameters.get("col1");
        String col2 = parameters.get("col2");
        String mag = parameters.get("mag");

        List<Row> cs1 = df.select(col1).collectAsList();
        List<Row> cs2 = df.select(col2).collectAsList();
        List<Row> cs3 = df.select(mag).collectAsList();

        // TODO: Please fix me because i don-t know why it works like that
        double[] c1 = toListDouble(cs1).stream().mapToDouble(d -> d).toArray();
        double[] c2 = toListDouble(cs2).stream().mapToDouble(d -> d).toArray();
        double[] c3 = toListDouble(cs3).stream().mapToDouble(d -> d).toArray();

        BubbleChart chart = new BubbleChartBuilder()
                .width(Integer.parseInt(parameters.getOrDefault("width", String.valueOf(width))))
                .height(Integer.parseInt(parameters.getOrDefault("height", String.valueOf(height))))
                .title(parameters.getOrDefault("title", title))
                .xAxisTitle(col1)
                .yAxisTitle(col2)
                .build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        // Series
        chart.addSeries(parameters.getOrDefault("sname", sname), c1, c2, c3);
        String path;
        /*if (StringUtils.containsIgnoreCase(os, "Windows")==true){
            Path tempPath = Files.createTempDirectory("graphs-");
            path = tempPath.toString();
        }
        else {
            path = "/tmp/graphs";
        }*/
        Path tempPath = Files.createTempDirectory("graphs-");
        path = tempPath.toString();

        //System.out.println(path);

        StringBuilder value = new StringBuilder().append(path).append(File.separator).append(parameters.get("fileName"));
        path = value.toString();

        System.out.println(path);


        BitmapEncoder.saveBitmap(chart, path, BitmapEncoder.BitmapFormat.PNG);
        //BitmapEncoder.saveBitmap(chart, "C:\\Users\\narda\\AppData\\Local\\Temp\\graphs-3640716319703269447\\samplefile.png", BitmapEncoder.BitmapFormat.PNG );
        HdfsWriter writer = new HdfsWriter(parameters);
        writer.createFile(parameters.get("folderName"), parameters.get("fileName"), path);

    //    HdfsWriter writer = new HdfsWriter();
    //    writer.createFile("/user/bda/graph","bubblechart.png", "D:\\ProgettiSpark\\graphtest\\xchart\\Sample_Chart_300_DPI.png");
    }

    public List<Double> toListDouble(List<Row> lrow){
        List<String> sq = new ArrayList<String>(lrow.size());
        for(Row str : lrow) {
            sq.add(String.valueOf(String.valueOf(str)));
        }
        String stq = sq.toString();
        stq = stq.replace("[", "").replace("]", "").replace(" ", "");
        List<String> qq = Arrays.stream(stq.split(",")).collect(Collectors.toList());
        List<Double> lq = new ArrayList<Double>(qq.size());
        for(String str : qq) {
            lq.add(Double.valueOf(str));
        }
        return lq;
    }

}
