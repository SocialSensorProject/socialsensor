package util;

import java.util.Comparator;
import java.util.Map;

/**
 * Created by zahraiman on 10/3/15.
 */
public class ValueComparator implements Comparator {
    Map<String, Double> base;

    public ValueComparator(Map base) {
        this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with
    // equals.
    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (base.get(o1.toString()) >= base.get(o2.toString())) {
            return -1;
        } else {
            return 1;
        }
    }
}