package it.unimi.evotion.tasks.anova;

import it.unimi.evotion.tasks.IAnova;
import it.unimi.evotion.tasks.util.Index;
import it.unimi.evotion.tasks.util.FRatioPValue;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.*;


public class BaseAnova implements IAnova, Serializable {

    // ----------------------------------------------------------------------
    // Spark dataset
    // ----------------------------------------------------------------------

    private Dataset<Row> df;

    // ----------------------------------------------------------------------
    // Private fields
    // ----------------------------------------------------------------------

    private String[] features;          // feature names
    private String label;               // label name
    private int nfeatures;              // n of features

    private Set[] levels;               // levels in each feature
    private int[] nlevels;              // n of levels in each feature

    private Map<Index, Long> counts;    // cell counts

    private Map<Index, Double> sums;    // cell sums
    private Map<Index, Double> means;   // cell means

    private Map<Index, Double> fsums;   // feature sums
    private Map<Index, Double> fmeans;  // feature means

    private Map<Index, Double> fratio;  // feature means
    private Map<Index, Double> pvalues; // feature means

    private long globalCount;           // globalCount number of items
    private double globalSum;           // globalCount sum
    private double globalMean;          // globalCount means

    private double sumOfSquaredWithin;  // SUM( SQ(y[ijk] - y[ij*]) )
    private long degreeWithin;          // global degree of freedom for within
    private double meanWithin;          // within mean

    private double sumOfSquaredTotal;
    private long degreeTotal;
    private double meanTotal;

    private double sumOfSquaredBetween; // SUM( SQ(y[ijk] - y[ij*]) )
    private long degreeBetween;         // between degree of freedom
    private double meanBetween;         // between mean

    // cross counts
    // cross means
    private Map<Index,Long> xcounts;
    private Map<Index,Double> xsums;
    private Map<Index,Double> xmeans;

    // ----------------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------------

    public BaseAnova(Dataset<Row> df, String label, String[] features) {

        this.df = df;
        this.label = label;
        this.features = features;

        int n = features.length;

        this.nfeatures = n;
        this.levels = new Set[n];
        this.nlevels = new int[n];
    }

    // ----------------------------------------------------------------------
    // Init
    // ----------------------------------------------------------------------

    @Override
    public void init() {
        logger().info("Initialize");

        checkFeatures();
        retrieveLevels();
        checkLevels();
    }

    // ----------------------------------------------------------------------
    // Implementation

    /**
     * Check if all selected features are valid columns
     * in the dataset
     */
    private void checkFeatures() {
        StructType schema = df.schema();

        // ceck the selected fiels
        schema.fieldIndex(label);
        for(int i=0; i<this.nfeatures; ++i)
            schema.fieldIndex(features[i]);
    }

    /**
     * Retrieve the levels in each feature
     */
    private void retrieveLevels() {

        // populate the data structure
        for(int i=0; i<this.nfeatures; ++i)
            levels[i] = new HashSet();

        // initialize 'columns' (NOT SERIALIZABLE)
        Column[] columns = columns(false);

        this.df.show();
        // populate the levels

        this.df
                .select(columns)
                .groupBy(columns)       // create the cells
                .count().show();

        List<Row> list = this.df
            .select(columns)
            .groupBy(columns)       // create the cells
            .count()           // count the number of elements for each cell
            .collectAsList();
        list
            .forEach(row -> {
                // levels
                for(int i=0; i<nfeatures; ++i)
                    levels[i].add(row.getString(i));
            })
        ;

        for(int i=0; i<nfeatures; ++i)
            nlevels[i] = levels[i].size();
    }

    /**
     * Check that EACH feature MUST have 2 or more levels
     * It is not possible to compute ANOVA on a feature with a single level
     */
    private void checkLevels() {
        List<String> wrongFeatures = new ArrayList<>();

        // each feature MUST have 2 or more levels
        for(int i=0; i<nfeatures; ++i)
            if (nlevels[i] <= 1)
                wrongFeatures.add(features[i]);

        if (wrongFeatures.size() > 0)
            throw new IllegalArgumentException(String.format("The features ( %s) have 0 or 1 level", wrongFeatures.toString()));
    }

    // ----------------------------------------------------------------------
    // Evaluate
    // ----------------------------------------------------------------------

    @Override
    public void evaluate() {
        logger().info("Evaluate");

        computeCellCounts();
        computeCellSums();
        computeCellMeans();
        computeCrossCounts();
        computeCrossSums();
        computeCrossMeans();
        computeMeanWithin();
        computeMeanBetween();
        computeFRatioPValue();
    }

    // ----------------------------------------------------------------------
    // Implementation

    private void computeCellCounts() {
        Column[] columns = columns(false);

        int ilabel = nfeatures;
        counts = new HashMap<>();
        globalCount = 0;

        List<Row> list = this.df
            .select(columns)
            .groupBy(columns)       // create the cells
            .count()                // count the number of elements for each cell
            .collectAsList();
        list
            .forEach(row -> {
                Index g = Index.index(row, nfeatures);
                long count = row.getLong(ilabel);

                counts.put(g, count);
                globalCount += count;
            })
        ;
    }
    private void computeCellSums() {
        Column[] columns = columns(false);
        Column[] allcols = columns(true);

        int ilabel = nfeatures;
        sums  = new HashMap<>();
        globalSum = 0;

        List<Row> list = this.df
            .select(allcols)
            .groupBy(columns)       // create the cells
            .sum(label)             // sum
            .collectAsList();
        list
            .forEach(row -> {
                Index g = Index.index(row, nfeatures);
                double s = row.getDouble(ilabel);
                globalSum += s;
                sums.put(g, s);
            })
        ;
    }

    private void computeCellMeans() {
        means = new HashMap<>();

        // compute cell means
        for(Index g : sums.keySet()) {
            long n = counts.get(g);
            double s = sums.get(g);

            means.put(g, s/n);
        }

        globalMean = globalSum / globalCount;
    }

    private void computeCrossCounts() {

        xcounts = new HashMap<>();

        // initialize xcounts
        for(Index g : counts.keySet()) {
            for(Index t : g.indices(true, true)) {
                xcounts.put(t, 0L);
            }
        }

        // compute xcounts
        for(Index g : counts.keySet()) {
            long n = counts.get(g);

            for(Index t : g.indices(true, true)) {
                xcounts.put(t, n + xcounts.get(t));
            }
        }
    }

    private void computeCrossSums() {

        xsums  = new HashMap<>();

        // initialize xsums
        for(Index g : means.keySet()) {
            for(Index t : g.indices(true, true)) {
                xsums.put(t, 0.);
            }
        }

        // compute xsums
        for(Index g : sums.keySet()) {
            double s = sums.get(g);

            for(Index t : g.indices(true, true)) {
                xsums.put(t, s + xsums.get(t));
            }
        }
    }

    private void computeCrossMeans() {

        xmeans = new HashMap<>();

        // initialize xmeans
        for(Index g : means.keySet()) {
            for(Index t : g.indices(true, true)) {
                xmeans.put(t, 0.);
            }
        }

        // normalize xmeans
        for(Index g : xsums.keySet()) {
            long n = xcounts.get(g);
            double s = xsums.get(g);

            xmeans.put(g, s/n);
        }
    }

    /**
     * sum of squared differences where the difference is between
     *
     *      the instance y
     *  and
     *
     *      the cell mean
     *
     * SUM(  SQ( y[ijk] - y[ij*])  )
     */
    private void computeMeanWithin() {
        int ilabel = nfeatures;

        // initialize columns (NOT SERIALIZABLE)
        Column[] allcols = columns(true);

        // compute SUM( SQ(y[ijk] - y[ij*]) )
        //                y-value - g-mean
        sumOfSquaredWithin = this.df
            .select(allcols)
            .javaRDD()
            .map(row -> {
                Index g = Index.index(row, nfeatures);
                double y = row.getDouble(ilabel);       // y[ijk]
                double m = means.get(g);                // y[ij*]

                return sq(y - m);
            })
            .reduce((v1, v2) -> v1+v2);

        // compute the freedom degree as the SUM of (n of items in each cell MINUS 1)
        //
        //  fw = SUM( counts[g]-1 )
        //
        degreeWithin = 0;
        for (Index g : counts.keySet())
            degreeWithin += counts.get(g)-1;

        meanWithin = sumOfSquaredWithin / degreeWithin;
    }

    /**
     * It is possible to separate this sum in two parts
     *
     *  1) related to each single feature
     *  2) related to the interactions between 2 or more features
     *
     */
    private void computeMeanBetween() {

        // initialize data structures
        fsums  = new HashMap<>();
        fmeans = new HashMap<>();

        for (Index f: Index.index(features).indices(true, true)) {
            fsums.put(f, 0.);
            fmeans.put(f, 0.);
        }

        // compute sum betweens

        sumOfSquaredBetween = 0;
        for(Index g : xmeans.keySet()) {

            double diff = xmeans.get(g);
            long n = xcounts.get(g);

            for(Index t : g.indices(true, false)) {
                int s = sign(g.rank(), t.rank());
                double m = xmeans.get(t);

                diff += s*m;
            }

            double s = n*sq(diff);

            Index f = g.convert(features);
            fsums.put(f, s + fsums.get(f));
        }

        // normalize
        sumOfSquaredBetween = 0;
        degreeBetween = 0;

        for(Index f : fsums.keySet()) {
            double s = fsums.get(f);
            int df = f.degreeOfFreedom(nlevels);

            sumOfSquaredBetween += s;
            degreeBetween += df;

            double m = s/df;

            fmeans.put(f, m);
        }

        meanBetween = sumOfSquaredBetween / degreeBetween;
    }

    private void computeFRatioPValue() {

        fratio = new HashMap<>();
        pvalues = new HashMap<>();

        for(Index g : fmeans.keySet()) {
            double msb = fmeans.get(g);
            double msw = meanWithin;

            double f = msb/msw;

            fratio.put(g, f);

            long df = g.degreeOfFreedom(nlevels);
            long dw = degreeWithin;
            double p = FRatioPValue.eval(f, df, dw);

            pvalues.put(g, p);
        }

    }

    // ----------------------------------------------------------------------
    // Result
    // ----------------------------------------------------------------------

    public AnovaResult result() {
        AnovaResult r = new AnovaResult();

        composeDegreeOfFreedom(r);
        composeSumOfSq(r);
        composeMeanSq(r);
        composeFRatioPValue(r);

        composeCellCounts(r);
        composeCellSums(r);
        composeCellMeans(r);

        return r;
    }

    // ----------------------------------------------------------------------
    // Implementation

    private void composeDegreeOfFreedom(AnovaResult r) {

        degreeTotal = degreeWithin;

        for(Index g : Index.index(features).indices(false, true)) {

            long df = g.degreeOfFreedom(nlevels);
            degreeTotal += df;

            String entry = g.asString();
            r.degreeOfFreedom.put(entry, df);
        }

        r.degreeOfFreedom.put("Within", degreeWithin);
        r.degreeOfFreedom.put("Between", degreeBetween);

        r.degreeOfFreedom.put("Error", degreeWithin);
        r.degreeOfFreedom.put("Total", degreeTotal);
    }

    private void composeSumOfSq(AnovaResult r) {
        sumOfSquaredTotal = sumOfSquaredWithin;

        for(Index g : fsums.keySet()) {
            if (g.empty()) continue;

            double s = fsums.get(g);
            sumOfSquaredTotal += s;

            String entry = g.asString();
            r.sumOfSquared.put(entry, s);
        }

        r.sumOfSquared.put("Within", sumOfSquaredWithin);
        r.sumOfSquared.put("Between", sumOfSquaredBetween);

        r.sumOfSquared.put("Error", sumOfSquaredWithin);
        r.sumOfSquared.put("Total", sumOfSquaredTotal);
    }

    private void composeMeanSq(AnovaResult r) {
        for(Index g : fsums.keySet()) {
            if (g.empty()) continue;

            double s = fsums.get(g);
            int df = g.degreeOfFreedom(nlevels);

            String entry = g.asString();
            r.meanSquared.put(entry, s/df);
        }

        meanTotal = sumOfSquaredTotal / degreeTotal;

        r.meanSquared.put("Within", meanWithin);
        r.meanSquared.put("Between", meanBetween);

        r.meanSquared.put("Error", meanWithin);
        r.meanSquared.put("Total", meanTotal);
    }

    private void composeFRatioPValue(AnovaResult r) {

        for(Index g : fratio.keySet()) {
            if (g.empty()) continue;

            double f = fratio.get(g);

            String entry = g.asString();
            r.fRatio.put(entry, f);
        }

        for(Index g : pvalues.keySet()) {
            if (g.empty()) continue;

            double p = pvalues.get(g);

            String entry = g.asString();
            r.pValue.put(entry, p);
        }
    }


    private void composeCellCounts(AnovaResult r) {
        for(Index g : xcounts.keySet()) {

            long n = xcounts.get(g);

            String entry = g.asString(features);
            r.cellCounts.put(entry, n);
        }
    }

    private void composeCellSums(AnovaResult r) {
        for(Index g : xsums.keySet()) {

            double s = xsums.get(g);

            String entry = g.asString(features);
            r.cellSums.put(entry, s);
        }
    }

    private void composeCellMeans(AnovaResult r) {
        for(Index g : xmeans.keySet()) {

            double m = xmeans.get(g);

            String entry = g.asString(features);
            r.cellMeans.put(entry, m);
        }
    }

    // ----------------------------------------------------------------------
    // Utilities
    // ----------------------------------------------------------------------

    private static int sign(int r1, int r2) {
        int r = r1 < r2 ? r2-r1 : r1-r2;
        return ((r&1) == 0) ? +1 : -1;
    }

    private static double sq(double x) { return x*x; }

    private Column[] columns(boolean withLabel) {
        Column[] cols = new Column[nfeatures + (withLabel ? 1 : 0)];

        for(int i=0; i<this.nfeatures; ++i) {
            cols[i] = df.col(features[i]);
        }
        if (withLabel) {
            int ilabel = nfeatures;
            cols[ilabel] = df.col(label);
        }

        return cols;
    }

    private Logger logger() {
        return Logger.getLogger(getClass());
    }

    // ----------------------------------------------------------------------
    // End
    // ----------------------------------------------------------------------

}
