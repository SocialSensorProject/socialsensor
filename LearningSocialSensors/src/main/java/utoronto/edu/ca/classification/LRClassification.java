/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.classification;

import de.bwaldvogel.liblinear.Feature;
import de.bwaldvogel.liblinear.FeatureNode;
import de.bwaldvogel.liblinear.Linear;
import de.bwaldvogel.liblinear.Model;
import de.bwaldvogel.liblinear.Parameter;
import de.bwaldvogel.liblinear.Problem;
import de.bwaldvogel.liblinear.SolverType;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import utoronto.edu.ca.data.DataSet;
import utoronto.edu.ca.util.Misc;
import utoronto.edu.ca.validation.Metrics;
import utoronto.edu.ca.validation.HyperParameters;
import static utoronto.edu.ca.validation.HyperParameters.NUM_FEATURES;
import static utoronto.edu.ca.validation.HyperParameters.nbr_features;
import static utoronto.edu.ca.validation.HyperParameters.C_values;

/**
 *
 * @author reda
 */
public class LRClassification {

    DataSet train;
    DataSet val;
    DataSet test;
    List<DataSet> list_test = new ArrayList<>();

    public LRClassification(String train, String val, String test) throws IOException {
        System.err.println("***********************************************************");
        System.err.println("Logistic Regression. ");
        System.err.println("***********************************************************");
        this.train = DataSet.readDataset(train, true, true);
        this.val = DataSet.readDataset(val, false, false);
        this.val.normalize(this.train.getColumn_stdev());
        this.test = DataSet.readDataset(test, false, false);
        this.test.normalize(this.train.getColumn_stdev());
        /**
         * Reading monthly test set.
         */
        for (int i = 1; i <= 20; i++) {
            String name = test.replaceAll(".csv", "");
            File tempFile = new File(name + i + ".csv");
            if (tempFile.exists()) {
                DataSet d = DataSet.readDataset(name + i + ".csv", false, false);
                d.normalize(this.train.getColumn_stdev());
                list_test.add(d);
            }
        }
        System.err.println(list_test.size() + " files are in test set.");
    }

    /**
     * This method tunes hyperparameters.
     *
     * @return
     */
    public HyperParameters tuneParameters() {
        System.err.println("***********************************************************");
        System.err.println("Number of parameters to fit: " + (C_values.length * nbr_features.length));
        System.err.println("***********************************************************");
        List<ImmutablePair<Double, Map<String, Double>>> gridsearch = new ArrayList<>();
        int[] feature_ranking = this.train.getIndexFeaturesRankingByMI();
        try (ProgressBar pb = new ProgressBar("Grid search", (C_values.length * nbr_features.length), ProgressBarStyle.ASCII)) {
            for (int nbr_feat : nbr_features) {
                FeatureNode[][] valx = this.val.getDatasetFeatureNode(feature_ranking, nbr_feat);
                double[] valy = this.val.getLables();
                for (double C : C_values) {
                    pb.step(); // step by 1
                    pb.setExtraMessage("Fitting parameters...");
                    Model model = getLRModel(feature_ranking, nbr_feat, C);
                    double[] y_probability_positive_class = new double[valy.length];
                    int positive_class_label = model.getLabels()[0];
                    for (int i = 0; i < valx.length; i++) {
                        Feature[] instance = valx[i];
                        double[] prob_estimates = new double[model.getNrClass()];
                        Linear.predictProbability(model, instance, prob_estimates);
                        y_probability_positive_class[i] = prob_estimates[0];
                    }
                    Metrics metric = new Metrics();
                    double ap = metric.getAveragePrecisionAtK(Misc.double2IntArray(valy), y_probability_positive_class, positive_class_label, 1000);
                    Map<String, Double> map = new HashMap<>();
                    map.put(HyperParameters.C, C);
                    map.put(NUM_FEATURES, (double) nbr_feat);
                    ImmutablePair<Double, Map<String, Double>> pair = new ImmutablePair<>(ap, map);
                    gridsearch.add(pair);
                }
            }
        }
        /**
         * Sorting based on best performance.
         */
        gridsearch.sort((ImmutablePair<Double, Map<String, Double>> pair1, ImmutablePair<Double, Map<String, Double>> pair2) -> {
            try {
                return Double.compare(pair2.left, pair1.left);
            } catch (Exception ex) {
                return -1;
            }
        });
        System.err.println("***********************************************************");
        System.err.println("[Best 10 parameters:");
        for (int i = 0; i < Math.min(10, gridsearch.size()); i++) {
            System.err.println((i + 1) + "- [C = " + gridsearch.get(i).right.get(HyperParameters.C) + ", Num features = "
                    + gridsearch.get(i).right.get(NUM_FEATURES) + "], AveP =  " + gridsearch.get(i).left);
        }
        System.err.println("***********************************************************");
        double C = gridsearch.get(0).right.get(HyperParameters.C);
        int num_features = (int) ((double) gridsearch.get(0).right.get(NUM_FEATURES));
        /**
         * Return best hyperparameters.
         */
        HyperParameters hp = new HyperParameters();
        hp.setValue_C(C);
        hp.setNum_features(num_features);
        hp.setFeature_ranking(feature_ranking);
        return hp;
    }

    /**
     * This method test the model.
     *
     * @param hyperparameters
     */
    public void testModel(HyperParameters hyperparameters) {
        /**
         * Train best model based on best hyperparameters.
         */
        Model model = getLRModel(hyperparameters.getFeature_ranking(), hyperparameters.getNum_features(), hyperparameters.getC());
        /**
         * Testing the model.
         */
        FeatureNode[][] testx = this.test.getDatasetFeatureNode(hyperparameters.getFeature_ranking(), hyperparameters.getNum_features());
        double[] testy = this.test.getLables();
        double[] y_probability_positive_class = new double[testy.length];
        int positive_class_label = model.getLabels()[0];
        for (int i = 0; i < testx.length; i++) {
            Feature[] instance = testx[i];
            double[] prob_estimates = new double[model.getNrClass()];
            Linear.predictProbability(model, instance, prob_estimates);
            y_probability_positive_class[i] = prob_estimates[0];
        }
        Metrics metric = new Metrics();
        double ap = metric.getAveragePrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 1000);
        double p10 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 10);
        double p100 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 100);
        double p1000 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 1000);
//        System.out.println("AP = " + ap);
//        System.out.println("P@10 = " + p10);
//        System.out.println("P@100 = " + p100);
//        System.out.println("P@1000 = " + p1000);
        System.out.println("#0\t" + ap + "\t" + p10 + "\t" + p100 + "\t" + p1000);
        int k = 0;
        for (DataSet local_test : this.list_test) {
            k++;
            testx = local_test.getDatasetFeatureNode(hyperparameters.getFeature_ranking(), hyperparameters.getNum_features());
            testy = local_test.getLables();
            y_probability_positive_class = new double[testy.length];
            positive_class_label = model.getLabels()[0];
            for (int i = 0; i < testx.length; i++) {
                Feature[] instance = testx[i];
                double[] prob_estimates = new double[model.getNrClass()];
                Linear.predictProbability(model, instance, prob_estimates);
                y_probability_positive_class[i] = prob_estimates[0];
            }
            metric = new Metrics();
            ap = metric.getAveragePrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 1000);
            p10 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 10);
            p100 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 100);
            p1000 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 1000);
            System.out.println(k + "\t" + ap + "\t" + p10 + "\t" + p100 + "\t" + p1000);
        }
    }

    /**
     * This methods train a model.
     *
     * @param feature_ranking
     * @param nbr_features
     * @param C
     * @return
     */
    private Model getLRModel(int[] feature_ranking, int nbr_features, double C) {
        FeatureNode[][] trainx = this.train.getDatasetFeatureNode(feature_ranking, nbr_features);
        double[] trainy = this.train.getLables();
        Problem problem = new Problem();
        // number of training examples
        problem.l = trainx.length;
        // number of features
        problem.bias = 1;
        problem.n = nbr_features + 1;
        // problem.x = ... // feature nodes
        problem.x = trainx;
        // problem.y = ... // target values
        problem.y = trainy;
        Linear.disableDebugOutput();
        SolverType solver = SolverType.L2R_LR; // -s 0
        double eps = 0.01; // stopping criteria
        Parameter parameter = new Parameter(solver, C, eps);
        return Linear.train(problem, parameter);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        LRClassification c = new LRClassification(args[0], args[1], args[2]);
        HyperParameters hyperparameters = c.tuneParameters();
        c.testModel(hyperparameters);
    }

}
