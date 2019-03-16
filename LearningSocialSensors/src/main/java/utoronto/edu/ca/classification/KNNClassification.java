/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.classification;

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
import static utoronto.edu.ca.validation.HyperParameters.k_values;
import static utoronto.edu.ca.validation.HyperParameters.nbr_features;
import weka.classifiers.lazy.IBk;
import weka.core.Instance;
import weka.core.Instances;

/**
 *
 * @author reda
 */
public class KNNClassification {

    DataSet train;
    DataSet val;
    DataSet test;

    public KNNClassification(String train, String val, String test) throws IOException {
        System.err.println("***********************************************************");
        System.err.println("KNN ");
        System.err.println("***********************************************************");
        this.train = DataSet.readDataset(train, true, true);
        this.val = DataSet.readDataset(val, false, false);
        this.val.normalize(this.train.getColumn_stdev());
        this.test = DataSet.readDataset(test, false, false);
        this.test.normalize(this.train.getColumn_stdev());
    }

    /**
     * This method tunes hyperparameters.
     *
     * @return
     * @throws java.lang.Exception
     */
    public HyperParameters tuneParameters() throws Exception {
        System.err.println("***********************************************************");
        System.err.println("Number of parameters to fit: " + (k_values.length * nbr_features.length));
        System.err.println("***********************************************************");
        List<ImmutablePair<Double, Map<String, Double>>> gridsearch = new ArrayList<>();
        int[] feature_ranking = this.train.getIndexFeaturesRankingByMI();
        try (ProgressBar pb = new ProgressBar("Grid search", (k_values.length * nbr_features.length), ProgressBarStyle.ASCII)) {
            for (int nbr_feat : nbr_features) {
                Instances train_instances = this.train.getDatasetInstances(feature_ranking, nbr_feat);
                Instances val_instances = this.val.getDatasetInstances(feature_ranking, nbr_feat);
                for (int k : k_values) {
                    pb.step(); // step by 1
                    pb.setExtraMessage("Fitting parameters...");
                    IBk knn = new IBk(k);

                    knn.buildClassifier(train_instances);
                    int positive_class_label = 1;
                    double[] valy = new double[val_instances.numInstances()];
                    double[] y_probability_positive_class = new double[val_instances.numInstances()];
                    for (int i = 0; i < val_instances.numInstances(); i++) {
                        Instance instance = val_instances.instance(i);
                        valy[i] = instance.classValue();
                        double[] v = knn.distributionForInstance(instance);
                        y_probability_positive_class[i] = v[1];
                    }
                    Metrics metric = new Metrics();
                    double ap = metric.getAveragePrecisionAtK(Misc.double2IntArray(valy), y_probability_positive_class, positive_class_label, 1000);
                    Map<String, Double> map = new HashMap<>();
                    map.put(HyperParameters.K, (double) k);
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
            System.err.println((i + 1) + "- [K = " + gridsearch.get(i).right.get(HyperParameters.K) + ", Num features = "
                    + gridsearch.get(i).right.get(NUM_FEATURES) + "], AveP =  " + gridsearch.get(i).left);
        }
        System.err.println("***********************************************************");
        int k = gridsearch.get(0).right.get(HyperParameters.K).intValue();
        int num_features = (int) ((double) gridsearch.get(0).right.get(NUM_FEATURES));
        /**
         * Return best hyperparameters.
         */
        HyperParameters hp = new HyperParameters();
        hp.setK(k);
        hp.setNum_features(num_features);
        hp.setFeature_ranking(feature_ranking);
        return hp;
    }

    /**
     * This method test the model.
     *
     * @param hyperparameters
     * @throws java.lang.Exception
     */
    public void testModel(HyperParameters hyperparameters) throws Exception {
        /**
         * Train best model based on best hyperparameters.
         */
        Instances train_instances = this.train.getDatasetInstances(hyperparameters.getFeature_ranking(), hyperparameters.getNum_features());
        IBk knn = new IBk(hyperparameters.getK());
        knn.buildClassifier(train_instances);
        /**
         * Testing the model.
         */
        Instances test_instances = this.test.getDatasetInstances(hyperparameters.getFeature_ranking(), hyperparameters.getNum_features());
        int positive_class_label = 1;
        double[] testy = new double[test_instances.numInstances()];
        double[] y_probability_positive_class = new double[test_instances.numInstances()];
        for (int i = 0; i < test_instances.numInstances(); i++) {
            Instance instance = test_instances.instance(i);
            testy[i] = instance.classValue();
            double[] v = knn.distributionForInstance(instance);
            y_probability_positive_class[i] = v[1];
        }
        Metrics metric = new Metrics();
        double ap = metric.getAveragePrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 1000);
        double p10 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 10);
        double p100 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 100);
        double p1000 = metric.getPrecisionAtK(Misc.double2IntArray(testy), y_probability_positive_class, positive_class_label, 1000);
        System.out.println("AP = " + ap);
        System.out.println("P@10 = " + p10);
        System.out.println("P@100 = " + p100);
        System.out.println("P@1000 = " + p1000);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, Exception {
        KNNClassification c = new KNNClassification(args[0], args[1], args[2]);
        HyperParameters hyperparameters = c.tuneParameters();
        c.testModel(hyperparameters);
    }

}
