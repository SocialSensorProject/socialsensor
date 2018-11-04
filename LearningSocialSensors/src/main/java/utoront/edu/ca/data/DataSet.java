/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoront.edu.ca.data;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lirmm.inria.fr.math.linear.BigSparseRealMatrix;
import lirmm.inria.fr.math.linear.OpenLongToDoubleHashMap;
import lirmm.inria.fr.math.linear.OpenMapRealVector;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooLargeException;

/**
 *
 * @author rbouadjenek
 */
public final class DataSet extends BigSparseRealMatrix {

    /**
     * Number of non zero entries in rows of the matrix.
     */
    private final OpenLongToDoubleHashMap rowNonZeroEntries = new OpenLongToDoubleHashMap(0.0);

    /**
     * Number of non zero entries in rows of the matrix.
     */
    private final OpenLongToDoubleHashMap columnNonZeroEntries = new OpenLongToDoubleHashMap(0.0);

    /**
     * Labels.
     */
    private final OpenMapRealVector labels;

    /**
     * Number of features in each category. The order here is important.
     */
    private final int term_features;
    private final int hashtag_features;
    private final int mention_features;
    private final int user_features;
    private final int loc_feature;
    /**
     * Rank of features.
     */
    public List<ImmutablePair<Integer, Double>> feature_ranking;

    /**
     * File Storing data.
     */
    private final File file;

    public static DataSet readDataset(String file) throws FileNotFoundException, IOException {
        BufferedReader brTest = new BufferedReader(new FileReader(file));
        String head = brTest.readLine();
        StringTokenizer st = new StringTokenizer(head, ",");
        int rows = Integer.parseInt(st.nextToken().split("=")[1]);
        int columns = Integer.parseInt(st.nextToken().split("=")[1]);
        int term_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int hashtag_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int mention_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int user_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int loc_feature = Integer.parseInt(st.nextToken().split("=")[1]);
        DataSet data = new DataSet(file, rows, columns, term_features, hashtag_features, mention_features, user_features, loc_feature);
        return data;
    }

    private DataSet(String file, int rowDimension, int columnDimension,
            int term_features, int hashtag_features, int mention_features, int user_features, int loc_feature) throws NotStrictlyPositiveException, NumberIsTooLargeException {
        super(rowDimension, columnDimension);
        labels = new OpenMapRealVector(rowDimension);
        this.term_features = term_features;
        this.hashtag_features = hashtag_features;
        this.mention_features = mention_features;
        this.user_features = user_features;
        this.loc_feature = loc_feature;
        this.file = new File(file);
        loadMatrix(file, rowDimension, columnDimension);
        normalize();
        rankFeatures();
    }

    protected void loadMatrix(String file, int rowDimension, int columnDimension) {
        FileInputStream fstream;
        try {
            fstream = new FileInputStream(file);
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            Set<Integer> cI = new HashSet();
            for (int i = 0; i < rowDimension; i++) {
                cI.add(i);
            }
            Set<Integer> cJ = new HashSet();
            for (int j = 0; j < columnDimension; j++) {
                cJ.add(j);
            }
            try (ProgressBar pb = new ProgressBar("Reading data from file", getRowDimension())) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                    String str;
                    int i = 0;
                    while ((str = br.readLine()) != null) {
                        str = str.trim();
                        if (str.startsWith("#")) {
                            continue;
                        }
                        if (str.trim().length() == 0) {
                            continue;
                        }
                        pb.step(); // step by 1
                        pb.setExtraMessage("Reading..."); // Set extra message to display at the end of the bar
                        Matcher m = Pattern.compile("\\s*(\\[[^\\]]*\\])|\\),[^\\]]*").matcher(str);
                        m.find();
                        String[] indices = m.group().replaceAll("\\[|\\]", "").split(",");
                        m.find();
                        String[] values = m.group().replaceAll("\\[|\\]", "").split(",");
                        m.find();
                        String label = m.group().replaceAll(",|\\)", "");
                        labels.setEntry(i, Double.parseDouble(label));
                        for (int k = 0; k < indices.length; k++) {
                            int j = Integer.parseInt(indices[k]);
                            double val = Double.parseDouble(values[k]);
                            setEntry(i, j, val);
                        }
                        i++;
                    }
                }
            }
            for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
                iterator.advance();
                final long key = iterator.key();
                final int i, j;
                if (isTransposed()) {
                    j = (int) (key / getRowDimension());
                    i = (int) (key % getRowDimension());
                } else {
                    i = (int) (key / getColumnDimension());
                    j = (int) (key % getColumnDimension());
                }
                double v = rowNonZeroEntries.get(i);
                rowNonZeroEntries.put(i, v + 1);
                v = columnNonZeroEntries.get(j);
                columnNonZeroEntries.put(j, v + 1);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This normalization divides only by stdev.
     */
    private void normalize() {
        double[] center = new double[getColumnDimension()];
        double[] scale = new double[getColumnDimension()];
        try (ProgressBar pb = new ProgressBar("Normalization", getEntries().size() * 3)) {
            /**
             * Computing the mean of each column.
             */
            for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
                pb.step(); // step by 1
                pb.setExtraMessage("Normalization in progress..."); // Set extra message to display at the end of the bar
                iterator.advance();
                final long key = iterator.key();
                final double value = iterator.value();
                final int j;
                if (isTransposed()) {
                    j = (int) (key / getRowDimension());
                } else {
                    j = (int) (key % getColumnDimension());
                }
                center[j] += value;
            }
            for (int j = 0; j < center.length; j++) {
                center[j] = center[j] / getRowDimension();
            }
            /**
             * Computing stdev for each column.
             */
            for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
                pb.step(); // step by 1
                pb.setExtraMessage("Normalization in progress..."); // Set extra message to display at the end of the bar
                iterator.advance();
                final double value = iterator.value();
                final long key = iterator.key();
                final int j;
                if (isTransposed()) {
                    j = (int) (key / getRowDimension());
                } else {
                    j = (int) (key % getColumnDimension());
                }
                scale[j] += Math.pow(value - center[j], 2);
            }

            for (int j = 0; j < scale.length; j++) {
                int columnZero = getRowDimension() - getColumnNonZeroEntry(j); // number of entries in column j with zeros.
                scale[j] += (columnZero * Math.pow(center[j], 2));
                scale[j] = Math.sqrt(scale[j] / (getRowDimension() - 1));
            }
            /**
             * Dividing by stdev.
             */
            for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
                pb.step(); // step by 1
                pb.setExtraMessage("Normalization in progress..."); // Set extra message to display at the end of the bar
                iterator.advance();
                final double value = iterator.value();
                final long key = iterator.key();
                final int i;
                final int j;
                if (isTransposed()) {
                    i = (int) (key % getRowDimension());
                    j = (int) (key / getRowDimension());
                } else {
                    i = (int) (key / getColumnDimension());
                    j = (int) (key % getColumnDimension());
                }
                setEntry(i, j, value / scale[j]);
            }
        }
    }

    public int getRowNonZeroEntry(int i) {
        if (isTransposed()) {
            return (int) columnNonZeroEntries.get(i);
        } else {
            return (int) rowNonZeroEntries.get(i);
        }
    }

    public int getColumnNonZeroEntry(int j) {
        if (isTransposed()) {
            return (int) rowNonZeroEntries.get(j);
        } else {
            return (int) columnNonZeroEntries.get(j);
        }
    }

    /**
     * This method returns a list of columns.
     *
     * @return
     */
    public List<OpenMapRealVector> getColumnVectorsAsList() {
        List<OpenMapRealVector> out = new ArrayList<>();
        for (int j = 0; j < getColumnDimension(); j++) {
            OpenMapRealVector column = new OpenMapRealVector(getRowDimension());
            out.add(column);
        }
        for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
            iterator.advance();
            final double value = iterator.value();
            final long key = iterator.key();
            final int i;
            final int j;
            if (isTransposed()) {
                i = (int) (key % getRowDimension());
                j = (int) (key / getRowDimension());
            } else {
                i = (int) (key / getColumnDimension());
                j = (int) (key % getColumnDimension());
            }
            OpenMapRealVector column = out.get(j);
            column.setEntry(i, value);
        }
        return out;
    }

    /**
     * Rank features using mutual information.
     */
    public void rankFeatures() {
        feature_ranking = new ArrayList<>();
        List<OpenMapRealVector> columns = getColumnVectorsAsList();
        double posClass = labels.getSparsity();
        double negClass = 1 - posClass;
        int j = 0;
        try (ProgressBar pb = new ProgressBar("Ranking Features", getColumnDimension())) {
            for (OpenMapRealVector c : columns) {
                //Compute correlation between c and labels.
                pb.step(); // step by 1
                pb.setExtraMessage("Computing Mutual Information..."); // Set extra message to display at the end of the bar

                double posFeature = c.getSparsity();
                double negFeature = 1 - posFeature;

                double posFeature_posClass = 0;
                double posFeature_negClass = 0;
                double negFeature_posClass = 0;
                double negFeature_negClass = 0;

                for (OpenLongToDoubleHashMap.Iterator iterator = c.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final int key = (int) iterator.key();
                    if (labels.getEntry(key) != 0) {
                        posFeature_posClass++;
                    } else {
                        posFeature_negClass++;
                    }
                }

                for (OpenLongToDoubleHashMap.Iterator iterator = labels.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final int key = (int) iterator.key();
                    if (c.getEntry(key) == 0) {
                        negFeature_posClass++;
                    }
                }
                negFeature_negClass = c.getDimension() - posFeature_posClass - posFeature_negClass - negFeature_posClass;

                posFeature_posClass = posFeature_posClass / c.getDimension();
                posFeature_negClass = posFeature_negClass / c.getDimension();
                negFeature_posClass = negFeature_posClass / c.getDimension();
                negFeature_negClass = negFeature_negClass / c.getDimension();

                double mi = posFeature_posClass * Math.log10(posFeature_posClass / (posFeature * posClass))
                        + posFeature_negClass * Math.log10(posFeature_negClass / (posFeature * negClass))
                        + negFeature_posClass * Math.log10(negFeature_posClass / (negFeature * posClass))
                        + negFeature_negClass * Math.log10(negFeature_negClass / (negFeature * negClass));
                if (!Double.isNaN(mi)) {
                    ImmutablePair<Integer, Double> pair = new ImmutablePair<>(j, mi);
                    feature_ranking.add(pair);
                }
                j++;
            }
        }
        feature_ranking.sort((ImmutablePair<Integer, Double> pair1, ImmutablePair<Integer, Double> pair2) -> {
            try {
                return Double.compare(pair1.right, pair2.right);
            } catch (Exception ex) {
                return -1;
            }
        });
    }

    /**
     * Print top features in different files for each category.
     *
     * @throws FileNotFoundException
     * @throws Exception
     */
    public void printTopFeaturesByCategory() throws FileNotFoundException, Exception {
        String filename = file.getName().split("\\.")[0];
        PrintWriter outWriterTerm_features = new PrintWriter(new FileOutputStream(new File(filename + "term_features.csv"), true));
        PrintWriter outWriterhashtag_features = new PrintWriter(new FileOutputStream(new File(filename + "hashtag_features.csv"), true));
        PrintWriter outWritermention_features = new PrintWriter(new FileOutputStream(new File(filename + "mention_features.csv"), true));
        PrintWriter outWriteruser_features = new PrintWriter(new FileOutputStream(new File(filename + "user_features.csv"), true));
        PrintWriter outWriterloc_feature = new PrintWriter(new FileOutputStream(new File(filename + "loc_feature.csv"), true));

        for (ImmutablePair<Integer, Double> pair : feature_ranking) {
            if (pair.getLeft() >= 0 && pair.getLeft() < term_features) {
                outWriterTerm_features.println(pair.right);
            } else if (pair.getLeft() >= term_features && pair.getLeft() < term_features + hashtag_features) {
                outWriterhashtag_features.println(pair.right);
            } else if (pair.getLeft() >= term_features + hashtag_features && pair.getLeft() < term_features + hashtag_features + mention_features) {
                outWritermention_features.println(pair.right);
            } else if (pair.getLeft() >= term_features + hashtag_features + mention_features && pair.getLeft() < term_features + hashtag_features + mention_features + user_features) {
                outWriteruser_features.println(pair.right);
            } else if (pair.getLeft() >= term_features + hashtag_features + mention_features + user_features && pair.getLeft() < term_features + hashtag_features + mention_features + user_features + loc_feature) {
                outWriterloc_feature.println(pair.right);
            } else {
                throw new Exception("Error in index.");
            }
        }
        outWriterTerm_features.close();
        outWriterhashtag_features.close();
        outWritermention_features.close();
        outWriteruser_features.close();
        outWriterloc_feature.close();
    }

}
