package it.unimi.evotion.tasks.rfc_predict;

public class rfRecord {

    private String name;
    private double value;

    public rfRecord(String n, double v){
        name = n;
        value = v;
    }

    public String getName() { return name; }
    public double getValue() { return value; }
}
