package it.unimi;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.avg;

/**
 * Hello world!
 */
public class SparkSample {
    /**
     * Spark Job that computes the average of the ages in the input dataset
     * 
     * @param args two positional arguments:
     *             - Input file path: a CSV file with header (id,name,age)
     *             - Output file path: a generic text file
     */
    public static void main(String[] args) {
        System.out.println("args[0]: " + args[0]);
        System.out.println("args[1]: " + args[1]);

        SparkSession spark = SparkSession
                .builder()
                .appName(SparkSample.class.getName())
                .getOrCreate();

        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.LongType, false, Metadata.empty())
        });

        var df = spark.read()
                .option("delimiter", ",")
                .option("header", "true")
                .schema(schema)
                .csv(args[0])
                .as(Encoders.bean(Person.class));

        var mappedDf = df.map((MapFunction<Person, Long>) p -> p.getAge(), Encoders.LONG());
        var outDf = mappedDf.agg(avg(mappedDf.col("value")).cast(DataTypes.StringType));

        System.out.println("Objects count: " + outDf.count());

        outDf.write().text(args[1]);
    }
}
