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

    double value_C;
    int value_K;
    double value_alpha;
    int num_features;
    int num_trees;
    int[] feature_ranking;

    public static final String C = "C";
    public static final String K = "K";
    public static final String ALPHA = "alpha";
    public static final String NUM_FEATURES = "num_features";
    public static final String FEATURE_RANKING = "feature_ranking";
    public static final String NUM_TREES = "nbr_trees";

    public static double[] C_values = new double[]{10e-12, 10e-11, 10e-10, 10e-9, 10e-8, 10e-7, 10e-6,
        10e-5, 10e-4, 10e-3, 10e-2, 10e-1, 10e0, 10e1, 10e2, 10e3, 10e4, 10e5, 10e6, 10e7, 10e8, 10e9,
        10e10, 10e11, 10e12,};

    public static int[] nbr_features = new int[]{100, 1000, 10000, 100000};

    public static double[] alpha_values = new double[]{10e-20, 10e-15, 10e-8, 10e-3, 10e-1, 1};

//    public static int[] nbr_trees = new int[]{10, 20, 50, 100, 200, 300, 400, 500, 600, 700};
    public static int[] nbr_trees = new int[]{2, 3};
    public static int[] k_values = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    public void setValue_C(double value_C) {
        this.value_C = value_C;
    }

    public int getK() {
        return value_K;
    }

    public void setK(int K) {
        this.value_K = K;
    }

    public void setValue_alpha(double value_alpha) {
        this.value_alpha = value_alpha;
    }

    public void setNum_trees(int num_trees) {
        this.num_trees = num_trees;
    }

    public void setNum_features(int num_features) {
        this.num_features = num_features;
    }

    public void setFeature_ranking(int[] feature_ranking) {
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

    public double getAlpha() {
        return value_alpha;
    }

    public int getNum_trees() {
        return num_trees;
    }

}
