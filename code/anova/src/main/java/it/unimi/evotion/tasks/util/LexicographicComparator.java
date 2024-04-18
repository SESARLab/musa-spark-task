package it.unimi.evotion.tasks.util;

import java.util.Comparator;

public class LexicographicComparator implements Comparator<String> {

    private static final String ALL = "All";

    private LexicographicComparator() { }


    @Override
    public int compare(String o1, String o2) {

        // special case: "All" must be the lowest element
        if (ALL.equals(o1) && ALL.equals(o2))
            return 0;
        if (ALL.equals(o1))
            return -1;
        if (ALL.equals(o2))
            return +1;

        int cmp;
        // cmp = o1.length() - o2.length();
        // if (cmp == 0)
            cmp = o1.compareTo(o2);
        return cmp;
    }

    public static LexicographicComparator COMPARATOR = new LexicographicComparator();
}
