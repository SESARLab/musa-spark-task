package it.unimi.evotion.tasks.anova;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class OneWayAnova extends BaseAnova {

    public OneWayAnova(Dataset<Row> df, String label, String[] features) {
        super(df, label, features);
    }

    @Override
    public void evaluate() {
        super.evaluate();
    }

}
