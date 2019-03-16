/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.validation;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author reda
 */
public class Metrics {

    /**
     * Compute the precision at k.
     *
     * @param truth
     * @param probability of class 1.
     * @param label
     * @param k
     * @return
     */
    public double getPrecisionAtK(int[] truth, double[] probability, int label, int k) {
        List<Bundle> list = new ArrayList<>();
        for (int i = 0; i < truth.length; i++) {
            list.add(new Bundle(truth[i], probability[i]));
        }
        list.sort((Bundle b1, Bundle b2) -> {
            try {
                double v1 = b1.getProbability();
                double v2 = b2.getProbability();
                return Double.compare(v2, v1);

            } catch (Exception ex) {
                return -1;
            }
        }
        );
        double p_k = 0;        
        double max = Math.min(list.size(), k);
        for (int i = 0; i < max; i++) {
            Bundle b = list.get(i);
            if (b.getTruth() == label) {
                p_k++;
            }
        }
        return p_k / k;
    }

    /**
     * Compute the precision at k.
     *
     * @param truth
     * @param probability of class 1.
     * @param label
     * @param k
     * @return
     */
    public double getAveragePrecisionAtK(int[] truth, double[] probability, int label, int k) {
        List<Bundle> list = new ArrayList<>();
        for (int i = 0; i < truth.length; i++) {
            list.add(new Bundle(truth[i], probability[i]));
        }
        list.sort((Bundle b1, Bundle b2) -> {
            try {
                double v1 = b1.getProbability();
                double v2 = b2.getProbability();
                return Double.compare(v2, v1);
            } catch (Exception ex) {
                return -1;
            }
        }
        );
        /**
         * Compute precision at each relevant element.
         */
        List<Double> p_list = new ArrayList<>();
        double rel = 0;
        double max = Math.min(list.size(), k);
        for (int i = 0; i < max; i++) {
            Bundle b = list.get(i);
            if (b.getTruth() == label) {
                rel++;
                p_list.add(rel / (i + 1));
            }
        }
        /**
         * Sum precisions and average.
         */
        double ap = 0;
        for (double v : p_list) {
            ap += v;
        }
        if (!p_list.isEmpty()) {
            ap = ap / p_list.size();
        }

        return ap;
    }

    class Bundle {

        double truth;
        double probability;

        public Bundle(double truth, double probability) {
            this.truth = truth;
            this.probability = probability;
        }

        public double getTruth() {
            return truth;
        }

        public double getProbability() {
            return probability;
        }

    }

}
