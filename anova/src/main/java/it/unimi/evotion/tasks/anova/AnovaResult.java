package it.unimi.evotion.tasks.anova;

import it.unimi.evotion.tasks.util.LexicographicComparator;

import java.util.Map;
import java.util.TreeMap;

public class AnovaResult {

    private static LexicographicComparator COMPARATOR = LexicographicComparator.COMPARATOR;

    public Map<String, Long> degreeOfFreedom = new TreeMap<>(COMPARATOR);
    public Map<String, Double> sumOfSquared = new TreeMap<>(COMPARATOR);
    public Map<String, Double> meanSquared = new TreeMap<>(COMPARATOR);
    public Map<String, Double> fRatio = new TreeMap<>(COMPARATOR);
    public Map<String, Double> pValue = new TreeMap<>(COMPARATOR);

    public Map<String, Long> cellCounts = new TreeMap<>(COMPARATOR);
    public Map<String, Double> cellSums = new TreeMap<>(COMPARATOR);
    public Map<String, Double> cellMeans = new TreeMap<>(COMPARATOR);

}
