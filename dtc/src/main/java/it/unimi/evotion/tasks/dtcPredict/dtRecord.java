package it.unimi.evotion.tasks.dtcPredict;

public class dtRecord {

    private String name;
    private double value;

    public dtRecord(String n, double v){
        name = n;
        value = v;
    }

    public String getName() { return name; }
    public double getValue() { return value; }
}
