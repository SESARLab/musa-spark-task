package it.unimi.evotion.tasks.svm;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import it.unimi.evotion.tasks.utils.CommonUtils;

public class SvmTask implements Task {

        // ----------------------------------------------------------------------
        // Private fields
        // ----------------------------------------------------------------------
        private Logger logger;
        private SparkSession spark;

        private String csvData;
        private String labelName;
        private String label;
        private int labelIndex;
        private String resultPath;
        private String maxIter;
        private String regParam;
        private List<String> lcn = new ArrayList<>();
        private Dataset<Row> df; // input dataframe
        private Dataset<Row> df2;
        private Dataset<Row> df3;
        private Dataset<Row> dfa;
        private Dataset<Row> dfResult;
        private Dataset<Row> dfResult2;

        private Vector featureImportances;

        // ----------------------------------------------------------------------
        // Interface implementation
        // ----------------------------------------------------------------------
        /**
         * Create the DecisionTree Model
         *
         * Note: if the label is numeric (float|double) the decision tree is used
         * for regressione, otherwise (int) for classification
         *
         * @param args
         *             args[0] url_csv input dataset (in CSV with header format)
         *             args[1] feature column names
         *             args[2] label column name (do not use 'label' because it is used
         *             by the task)
         *             args[3] number of maximum iterations
         *             args[4] regression parameter
         *             args[5] url_json|url_csv result of SVM information
         * @throws Exception
         */
        @Override
        public void init(Object... args) throws Exception {

                this.logger = LogManager.getLogger(SvmTask.class);

                if (args.length < 6)
                        throw new InvalidParameterException("Missing parameters.");

                this.csvData = (String) args[0];
                this.labelName = (String) args[1];
                this.label = (String) args[2];
                this.maxIter = (String) args[3];
                this.regParam = (String) args[4];
                this.labelIndex = -1;
                this.resultPath = (String) args[5];

                try {
                        this.spark = SparkSession
                                        .builder()
                                        // .master("local[2]")
                                        // .master("yarn")
                                        .appName("LinReg")
                                        .getOrCreate();
                } catch (Exception e) {
                        this.spark = SparkSession
                                        .builder()
                                        .master("local[2]")
                                        // .master("yarn")
                                        .appName("LinReg")
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

                LinearSVC lsvc = new LinearSVC()
                                .setMaxIter(Integer.parseInt(this.maxIter))
                                .setRegParam(Double.parseDouble(this.regParam));

                String processedLabelNames = this.labelName.replaceAll("'", "");

                StringIndexer indexer = new StringIndexer()
                                .setInputCol(String.valueOf(df.col(this.label)))
                                .setOutputCol("label");
                Dataset<Row> dfi = indexer.fit(df).transform(df);
                // dfi.show();

                df2 = dfi;
                for (String c : df2.columns()) {
                        df2 = df2.withColumnRenamed(c, c.replaceAll("'", ""));
                }
                df2.createOrReplaceTempView("dft");
                df3 = spark.sql(String.format("SELECT %s, label FROM dft", processedLabelNames));

                this.lcn = Arrays.stream(processedLabelNames.split(",")).collect(Collectors.toList());
                String[] names = this.lcn.stream().toArray(String[]::new);

                VectorAssembler assembler1 = new VectorAssembler()
                                .setInputCols(names)
                                .setOutputCol("features");

                dfa = assembler1.transform(df3);
                // dfa.show();

                // Fit the model
                LinearSVCModel lsvcModel = lsvc.fit(dfa);

                // Print the coefficients and intercept for LinearSVC

                double[] coef = lsvcModel.coefficients().toArray();
                List<Row> co = new ArrayList<>();
                for (double v1 : coef) {
                        co.add(RowFactory.create(v1));
                }

                List<Row> row = Arrays.asList(
                                RowFactory.create(lsvcModel.intercept()));

                StructType sch = new StructType(new StructField[] {
                                new StructField("Coefficients", DataTypes.DoubleType, true, Metadata.empty()) });
                dfResult = spark.createDataFrame(co, sch);

                StructType sch2 = new StructType(new StructField[] {
                                new StructField("Intercept", DataTypes.DoubleType, true, Metadata.empty()) });
                dfResult2 = spark.createDataFrame(row, sch2);

        }

        private void convertFeatures() {
                List<String> stringFeatures = new ArrayList<>();

                for (StructField field : this.df.schema().fields()) {
                        if (field.dataType().equals(DataTypes.StringType))
                                stringFeatures.add(field.name());
                }

                for (String sf : stringFeatures) {
                        String rn = "str-" + sf;
                        this.df = this.df.withColumnRenamed(sf, rn);
                        StringIndexer encoder = new StringIndexer()
                                        .setInputCol(rn)
                                        .setOutputCol(sf);

                        this.df = encoder.fit(this.df).transform(this.df).drop(rn);
                        this.df = this.df.withColumn(sf, this.df.col(sf).cast(DataTypes.IntegerType));
                }

                this.df.printSchema();
                // this.df.show();
        }

        @Override
        public void postProcessing(Object... params) throws Exception {

                this.dfResult.coalesce(1).write()
                                .format("csv")
                                // .mode(SaveMode.Overwrite)
                                .option("header", "true")
                                .save(resultPath + "/coefficients");

                this.dfResult2.coalesce(1).write()
                                .format("csv")
                                // .mode(SaveMode.Append)
                                .option("header", "true")
                                .save(resultPath + "/intercept");

                CommonUtils.sleepIfSystemPropIsSet();
                this.spark.stop();
                this.spark.close();
        }
}