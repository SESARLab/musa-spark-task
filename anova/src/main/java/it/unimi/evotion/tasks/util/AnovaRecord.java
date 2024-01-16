package it.unimi.evotion.tasks.util;

public class AnovaRecord {

    public static AnovaRecord create(String c, String n, double v) {
        return new AnovaRecord(c,n,v);
    }

    private String category;
    private String name;
    private double value;

    public AnovaRecord(String c, String n, double v) {
        category = c;
        name = n;
        value = v;
    }

    public String getCategory() {
        return category;
    }

    public String getName() {
        return name;
    }

    public double getValue() {
        return value;
    }

}
