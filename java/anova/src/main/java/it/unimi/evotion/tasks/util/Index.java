package it.unimi.evotion.tasks.util;


import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Index implements Serializable {

    // ----------------------------------------------------------------------
    // Factory Methods
    // ----------------------------------------------------------------------

    public static Index index(String[] values) {
        return new Index(values);
    }

    public static Index index(Row row, int n) {
        String[] values = new String[n];
        for(int i=0; i<n; ++i)
            values[i] = row.getString(i);
        return new Index(values);
    }


    // ----------------------------------------------------------------------
    // Private Fields
    // ----------------------------------------------------------------------

    private static final String STAR = "*";

    private String[] values;
    private List<Integer> indices;


    // ----------------------------------------------------------------------
    // Constructor
    // ----------------------------------------------------------------------

    private Index(String[] values) {
        this.values = values;
        this.indices = new ArrayList<>();

        for(int i=0; i<values.length; ++i)
            indices.add(i);
    }

    private Index(List<Integer> indices, String[] values) {
        this.indices = indices;
        this.values = values;
    }


    // ----------------------------------------------------------------------
    // Properties
    // ----------------------------------------------------------------------

    public int rank() { return indices.size(); }

    public boolean hasIndex(int i) {
        return indices.contains(i);
    }

    public boolean empty() { return indices.isEmpty(); }


    // ----------------------------------------------------------------------
    // Extras
    // ----------------------------------------------------------------------

    public int degreeOfFreedom(int[] nlevels) {
        int df = 1;
        for(int i : indices)
            df *= (nlevels[i] - 1);
        return df;
    }

    public Index convert(String[] values) {
        return new Index(this.indices, values);
    }

    // ----------------------------------------------------------------------
    // Iterations
    // ----------------------------------------------------------------------

    public Iterable<Index> indices() {
        return indices(true, true);
    }

    public Iterable<Index> indices(boolean es, boolean fs) {
        //return new Iter(es, fs);
        int rmin = es ? 0 : 1;
        int rmax = fs ? rank() : rank()-1;
        return new RankIter(rmin, rmax);
    }

    public Iterable<Index> indices(int rmin, int rmax) {
        return new RankIter(rmin, rmax);
    }


    // ----------------------------------------------------------------------
    // Overrides
    // ----------------------------------------------------------------------

    @Override
    public int hashCode() {
        int hc = indices.size();
        int j=0;
        for(int i : indices) {
            hc ^= ((values[i].hashCode() ^ i) << j);
            j = (j+1)%16;
        }
        return hc;
    }

    @Override
    public boolean equals(Object other) {
        Index that = (Index)other;
        if (this.rank() != that.rank())
            return false;

        for(int i=0; i<indices.size(); ++i)
            if (!indices.get(i).equals(that.indices.get(i)))
                return false;

        for(int i : indices)
            if (!this.values[i].equals(that.values[i]))
                return false;

        return true;
    }


    // ----------------------------------------------------------------------
    // Format
    // ----------------------------------------------------------------------

    public String asString() {
        if(indices.size() == 0)
            return "All";

        StringBuffer sb = new StringBuffer();
        for(int i : indices) {
            if (sb.length() > 0) sb.append(",");
            sb.append(values[i]);
        }
        return sb.toString();
    }

    public String asString(String[] names) {
        if(indices.size() == 0)
            return "All";

        StringBuffer sb = new StringBuffer();
        for(int i : indices) {
            if (sb.length() > 0) sb.append(",");
            sb.append(String.format("%s[%s]", names[i], values[i]));
        }
        return sb.toString();
    }


    @Override
    public String toString() {
        int n = values.length;
        StringBuffer sb = new StringBuffer();

        for(int i=0; i<n; ++i) {
            if (sb.length() > 0) sb.append(",");
            if (indices.contains(i))
                sb.append(values[i]);
            else
                sb.append(STAR);
        }

        sb.append(String.format(" (%d)", hashCode()));
        return sb.toString();
    }


    // ----------------------------------------------------------------------
    // Implementation
    // ----------------------------------------------------------------------

    private class Iter implements Iterable<Index>, Iterator<Index> {

        private int n;
        private int curr;
        private int nmax;

        private Iter(boolean es, boolean fs) {
            n = Index.this.indices.size();
            curr = es ? 0 : 1;
            nmax = (1 << n) - (fs ? 0 : 1);
        }

        @Override
        public Iterator<Index> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return curr < nmax;
        }

        @Override
        public Index next() {
            int mask = curr;
            curr++;
            List<Integer> indices = new ArrayList<>();
            for(int i=0; i<n; ++i)
                if ((mask & (1<<i)) != 0)
                    indices.add(Index.this.indices.get(i));

            return new Index(indices, Index.this.values);
        }

    }

    private class RankIter implements Iterable<Index>, Iterator<Index> {

        private int rmin;
        private int rmax;
        private Iter it;
        private Index curr;

        private RankIter(int min, int max) {
            rmin = min;
            rmax = max;
            it = new Iter(true, true);

            findValid();
        }

        @Override
        public Iterator<Index> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return curr != null;
        }

        @Override
        public Index next() {
            Index index = curr;
            findValid();
            return index;
        }

        private void findValid() {
            curr = null;
            while (it.hasNext()) {
                Index index = it.next();
                int rank = index.rank();
                if (rmin <= rank && rank <= rmax) {
                    curr = index;
                    break;
                }
            }
        }
    }
}
