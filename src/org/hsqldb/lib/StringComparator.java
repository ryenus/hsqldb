package org.hsqldb.lib;

public class StringComparator implements ObjectComparator {

    public int compare(Object a, Object b) {

        // handle nulls
        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        return ((String) a).compareTo((String) b);
    }
}
