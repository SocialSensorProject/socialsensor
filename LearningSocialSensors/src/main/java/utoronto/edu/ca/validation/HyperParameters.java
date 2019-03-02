/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.validation;

/**
 * Class to represent a set of parameters.
 *
 * @author reda
 */
public class HyperParameters {

    final double value_C;
    final int num_features;
    final int[] feature_ranking;

    public static final String C = "C";
    public static final String NUM_FEATURES = "num_features";
    public static final String FEATURE_RANKING = "feature_ranking";

    public static double[] C_values = new double[]{10e-12, 10e-11, 10e-10, 10e-9, 10e-8, 10e-7, 10e-6,
        10e-5, 10e-4, 10e-3, 10e-2, 10e-1, 10e0, 10e1, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7, 10e8, 10e9,
        10e10, 10e11, 10e12,};

    public static int[] nbr_features = new int[]{100, 1000, 10000, 100000};

    public static double[] alpha_values = new double[]{10e-20, 10e-15, 10e-8, 10e-3, 10e-1, 1};

    public HyperParameters(double C, int num_features, int[] feature_ranking) {
        this.value_C = C;
        this.num_features = num_features;
        this.feature_ranking = feature_ranking;
    }

    public double getC() {
        return value_C;
    }

    public int getNum_features() {
        return num_features;
    }

    public int[] getFeature_ranking() {
        return feature_ranking;
    }
}
