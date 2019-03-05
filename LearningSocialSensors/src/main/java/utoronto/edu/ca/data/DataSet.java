/**
 * Copyright (c) 2018 Reda Bouadjenek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package utoronto.edu.ca.data;

import de.bwaldvogel.liblinear.FeatureNode;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lirmm.inria.fr.math.linear.OpenIntToFloatHashMap;
import lirmm.inria.fr.math.linear.OpenMapRealVector;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarStyle;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooLargeException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.util.FastMath;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.SparseInstance;

/**
 * This class represents a dataset. It applies normalization to the dataset by
 * dividing by the standard deviation. I also allows to compute mutual
 * information and ranking features consequently.
 *
 * @author rbouadjenek
 */
public final class DataSet {

    /**
     * Number of non zero entries in rows of the matrix.
     */
    private final OpenIntToFloatHashMap rowNonZeroEntries = new OpenIntToFloatHashMap(0f);

    /**
     * Number of non zero entries in rows of the matrix.
     */
    private final OpenIntToFloatHashMap columnNonZeroEntries = new OpenIntToFloatHashMap(0f);

    /**
     * Labels of each instance (0,1).
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
     * Number of rows of the matrix.
     */
    private final int rows;
    /**
     * Storage for (sparse) matrix elements by column -- each element in the
     * array is a column.
     */
    private final OpenMapRealVector[] entries;
    /**
     * Rank of features.
     */
    public List<ImmutablePair<Integer, Double>> feature_ranking;

    /**
     * File Storing the data.
     */
    private final File file;
    /**
     * Mean of columns.
     */
    double[] center;

    /**
     * Stdev of colums.
     */
    double[] column_stdev;

    /**
     * Method to read a specific file and create a DataSet object.
     *
     * @param file
     * @param normalize
     * @param rankfeatures
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static DataSet readDataset(String file, boolean normalize, boolean rankfeatures) throws FileNotFoundException, IOException {
        FileInputStream fstream = new FileInputStream(file);
        System.err.println("***********************");
        System.err.println("Processing: " + file);
        System.err.println("***********************");
        // Get the object of DataInputStream
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String head = br.readLine();
        StringTokenizer st = new StringTokenizer(head, ",");
        int rows = Integer.parseInt(st.nextToken().split("=")[1]);
        int columns = Integer.parseInt(st.nextToken().split("=")[1]);
        int term_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int hashtag_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int mention_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int user_features = Integer.parseInt(st.nextToken().split("=")[1]);
        int loc_feature = Integer.parseInt(st.nextToken().split("=")[1]);
        DataSet data = new DataSet(file, rows, columns, term_features, hashtag_features, mention_features, user_features, loc_feature, normalize, rankfeatures);
        return data;
    }

    /**
     * Private constructor to create a DataSet object.
     *
     * @param file
     * @param rowDimension
     * @param columnDimension
     * @param term_features
     * @param hashtag_features
     * @param mention_features
     * @param user_features
     * @param loc_feature
     * @throws NotStrictlyPositiveException
     * @throws NumberIsTooLargeException
     */
    private DataSet(String file, int rowDimension, int columnDimension,
            int term_features, int hashtag_features, int mention_features, int user_features, int loc_feature, boolean normalize, boolean rankfeatures) throws NotStrictlyPositiveException, NumberIsTooLargeException {
        if (rowDimension * columnDimension >= Long.MAX_VALUE) {
            throw new NumberIsTooLargeException(rowDimension * columnDimension, Long.MAX_VALUE, false);
        }
        this.entries = new OpenMapRealVector[columnDimension];
        this.rows = rowDimension;
        for (int j = 0; j < columnDimension; j++) {
            this.entries[j] = new OpenMapRealVector(rowDimension);
        }
        labels = new OpenMapRealVector(rowDimension);
        this.term_features = term_features;
        this.hashtag_features = hashtag_features;
        this.mention_features = mention_features;
        this.user_features = user_features;
        this.loc_feature = loc_feature;
        this.file = new File(file);
        center = new double[columnDimension];
        loadMatrix();
        if (normalize) {
            normalize();
        }
        if (rankfeatures) {
            rankFeatures();
        }
    }

    /**
     * Method that reads a file.
     *
     * @param file
     */
    private void loadMatrix() {
        FileInputStream fstream;
        try {
            fstream = new FileInputStream(file);
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            try (ProgressBar pb = new ProgressBar("Reading data from file", getRowDimension(), ProgressBarStyle.ASCII)) {
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
                        pb.step();
                        pb.setExtraMessage("Reading...");
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
                            /**
                             * Counting number of non zeros in rows and columns.
                             */
                            float v = rowNonZeroEntries.get(i);
                            rowNonZeroEntries.put(i, v + 1);
                            v = columnNonZeroEntries.get(j);
                            columnNonZeroEntries.put(j, v + 1);
                            /**
                             * Counting mean of columns.
                             */
                            center[j] += val;
                        }
                        i++;
                    }
                    /**
                     * Computing the mean of each column.
                     */
                    for (int j = 0; j < center.length; j++) {
                        center[j] = center[j] / getRowDimension();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the entries in column number {@code column} as a vector. Row
     * indices start at 0.
     *
     * @param column column to be fetched.
     * @return a column vector.
     * @throws OutOfRangeException if the specified column index is invalid.
     */
    public OpenMapRealVector getColumnVector(final int column) throws OutOfRangeException {
        checkColumnIndex(column);
        return entries[column];
    }

    /**
     * This normalization divides only by stdev.
     */
    private void normalize() {
        column_stdev = new double[getColumnDimension()];
        try (ProgressBar pb = new ProgressBar("Normalization", getColumnDimension() * 2, ProgressBarStyle.ASCII)) {
            /**
             * Computing stdev for each column.
             */
            for (int j = 0; j < getColumnDimension(); j++) {
                pb.step(); // step by 1
                pb.setExtraMessage("Compute stdev for each column...");
                OpenMapRealVector column = getColumnVector(j);
                for (OpenIntToFloatHashMap.Iterator iterator = column.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final double value = iterator.value();
                    column_stdev[j] += Math.pow(value - center[j], 2);
                }
            }

            for (int j = 0; j < column_stdev.length; j++) {
                int columnZero = getRowDimension() - getColumnNonZeroEntry(j); // number of entries in column j with zeros.
                column_stdev[j] += (columnZero * Math.pow(center[j], 2));
                column_stdev[j] = Math.sqrt(column_stdev[j] / (getRowDimension() - 1));
            }
            /**
             * Dividing by stdev.
             */
            for (int j = 0; j < getColumnDimension(); j++) {
                pb.step();
                pb.setExtraMessage("Dividing by stdev...");
                OpenMapRealVector column = getColumnVector(j);
                for (OpenIntToFloatHashMap.Iterator iterator = column.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final double value = iterator.value();
                    final int i = (int) iterator.key();
                    setEntry(i, j, value / column_stdev[j]);
                }
            }
        }
    }

    /**
     * This normalization divides only by stdev.
     *
     * @param column_stdev
     */
    public void normalize(double[] column_stdev) {
        /**
         * Dividing by stdev.
         */
        try (ProgressBar pb = new ProgressBar("Normalization", getColumnDimension(), ProgressBarStyle.ASCII)) {
            for (int j = 0; j < getColumnDimension(); j++) {
                pb.step();
                pb.setExtraMessage("Dividing by stdev...");
                OpenMapRealVector column = getColumnVector(j);
                for (OpenIntToFloatHashMap.Iterator iterator = column.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final double value = iterator.value();
                    final int i = (int) iterator.key();
                    setEntry(i, j, value / column_stdev[j]);
                }
            }
        }
    }

    /**
     * Return the number of non zero entries in a given row.
     *
     * @param i
     * @return
     */
    public int getRowNonZeroEntry(int i) {
        return (int) rowNonZeroEntries.get(i);

    }

    /**
     * Return the number of non zero entries in a given column.
     *
     * @param j
     * @return
     */
    public int getColumnNonZeroEntry(int j) {
        return (int) columnNonZeroEntries.get(j);

    }

    /**
     * Returns the number of rows of this matrix.
     *
     * @return the number of rows.
     */
    public int getRowDimension() {
        return rows;

    }

    /**
     * Returns the number of columns of this matrix.
     *
     * @return the number of columns.
     */
    public int getColumnDimension() {
        return entries.length;

    }

    /**
     * Set the entry in the specified row and column. Row and column indices
     * start at 0.
     *
     * @param row Row index of entry to be set.
     * @param column Column index of entry to be set.
     * @param value the new value of the entry.
     * @throws OutOfRangeException if the row or column index is not valid
     * @since 2.0
     */
    public void setEntry(int row, int column, double value)
            throws OutOfRangeException {
        checkColumnIndex(column);
        checkRowIndex(row);
        entries[column].setEntry(row, value);
    }

    /**
     * Return the entry in the specified row and column. Row and column indices
     * start at 0.
     *
     * @param row Row index of entry to be set.
     * @param column Column index of entry to be set.
     * @return
     * @throws OutOfRangeException if the row or column index is not valid
     * @since 2.0
     */
    public double getEntry(int row, int column)
            throws OutOfRangeException {
        checkColumnIndex(column);
        checkRowIndex(row);
        return entries[column].getEntry(row);
    }

    /**
     * Check if a column index is valid.
     *
     * @param column Column index to check.
     * @throws OutOfRangeException if {@code column} is not a valid index.
     */
    public void checkColumnIndex(final int column)
            throws OutOfRangeException {
        if (column < 0 || column >= getColumnDimension()) {
            throw new OutOfRangeException(LocalizedFormats.COLUMN_INDEX,
                    column, 0, getColumnDimension() - 1);
        }
    }

    /**
     * Check if a row index is valid.
     *
     * @param row Row index to check.
     * @throws OutOfRangeException if {@code row} is not a valid index.
     */
    public void checkRowIndex(final int row)
            throws OutOfRangeException {
        if (row < 0
                || row >= getRowDimension()) {
            throw new OutOfRangeException(LocalizedFormats.ROW_INDEX,
                    row, 0, getRowDimension() - 1);
        }
    }

    /**
     * Rank features using Mutual Information.
     */
    public void rankFeatures() {
        feature_ranking = new ArrayList<>();
        double size_posClass = labels.getEntries().size();
        double size_negClass = labels.getDimension() - size_posClass;
        int j = 0;
        try (ProgressBar pb = new ProgressBar("Ranking Features", getColumnDimension(), ProgressBarStyle.ASCII)) {
            for (OpenMapRealVector c : entries) {
                pb.step();
                pb.setExtraMessage("Computing Mutual Information...");

                double posFeature = c.getSparsity();
                double negFeature = 1 - posFeature;

                double posFeature_posClass = 0;
                double posFeature_negClass = 0;

                for (OpenIntToFloatHashMap.Iterator iterator = c.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final int key = (int) iterator.key();
                    if (labels.getEntry(key) != 0) {
                        posFeature_posClass++;
                    } else {
                        posFeature_negClass++;
                    }
                }

                double negFeature_posClass = size_posClass - posFeature_posClass;
                double negFeature_negClass = c.getDimension() - posFeature_posClass - posFeature_negClass - negFeature_posClass;

                /**
                 * Transforming to probabilities.
                 */
                double posClass = size_posClass / c.getDimension();
                double negClass = size_negClass / c.getDimension();
                posFeature_posClass = posFeature_posClass / c.getDimension();
                posFeature_negClass = posFeature_negClass / c.getDimension();
                negFeature_posClass = negFeature_posClass / c.getDimension();
                negFeature_negClass = negFeature_negClass / c.getDimension();

                double mi = 0;

                if (posFeature_posClass != 0) {
                    mi += posFeature_posClass * FastMath.log(2, posFeature_posClass / (posFeature * posClass));
                }
                if (posFeature_negClass != 0) {
                    mi += posFeature_negClass * FastMath.log(2, posFeature_negClass / (posFeature * negClass));
                }
                if (negFeature_posClass != 0) {
                    mi += negFeature_posClass * FastMath.log(2, negFeature_posClass / (negFeature * posClass));
                }
                if (negFeature_negClass != 0) {
                    mi += negFeature_negClass * FastMath.log(2, negFeature_negClass / (negFeature * negClass));
                }
                if (!Double.isNaN(mi)) {
                    ImmutablePair<Integer, Double> pair = new ImmutablePair<>(j, mi);
                    feature_ranking.add(pair);
                }
                j++;
            }
        }
        feature_ranking.sort((ImmutablePair<Integer, Double> pair1, ImmutablePair<Integer, Double> pair2) -> {
            try {
                return Double.compare(pair2.right, pair1.right);
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
        PrintWriter outWriterTerm_features = new PrintWriter(new FileOutputStream(new File(filename + "_term_features.txt"), true));
        PrintWriter outWriterhashtag_features = new PrintWriter(new FileOutputStream(new File(filename + "_hashtag_features.txt"), true));
        PrintWriter outWritermention_features = new PrintWriter(new FileOutputStream(new File(filename + "_mention_features.txt"), true));
        PrintWriter outWriteruser_features = new PrintWriter(new FileOutputStream(new File(filename + "_user_features.txt"), true));
        PrintWriter outWriterloc_feature = new PrintWriter(new FileOutputStream(new File(filename + "_loc_features.txt"), true));

        for (ImmutablePair<Integer, Double> pair : feature_ranking) {
            if (pair.getLeft() >= 0 && pair.getLeft() < term_features) {
                outWriterTerm_features.println(pair.left + "\t" + pair.right);
            } else if (pair.getLeft() >= term_features && pair.getLeft() < term_features + hashtag_features) {
                outWriterhashtag_features.println(pair.left + "\t" + pair.right);
            } else if (pair.getLeft() >= term_features + hashtag_features && pair.getLeft() < term_features + hashtag_features + mention_features) {
                outWritermention_features.println(pair.left + "\t" + pair.right);
            } else if (pair.getLeft() >= term_features + hashtag_features + mention_features && pair.getLeft() < term_features + hashtag_features + mention_features + user_features) {
                outWriteruser_features.println(pair.left + "\t" + pair.right);
            } else if (pair.getLeft() >= term_features + hashtag_features + mention_features + user_features && pair.getLeft() < term_features + hashtag_features + mention_features + user_features + loc_feature) {
                outWriterloc_feature.println(pair.left + "\t" + pair.right);
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

    /**
     * This method returns the indexes of top features.
     *
     * @return
     */
    public int[] getIndexFeaturesRankingByMI() {
        int[] out = new int[feature_ranking.size()];
        for (int i = 0; i < out.length; i++) {
            ImmutablePair<Integer, Double> pair = feature_ranking.get(i);
            out[i] = pair.getLeft();
        }
        return out;
    }

    /**
     * This method returns a dataset using a FeatureNode array structure, with
     * k-features selected from indexes given using featureIndexes.
     *
     * @param featureIndexes
     * @param k
     * @return
     */
    public FeatureNode[][] getDatasetFeatureNode(int[] featureIndexes, int k) {
        List<FeatureNode>[] data = new ArrayList[getRowDimension()];
        for (int i = 0; i < data.length; i++) {
            data[i] = new ArrayList<>();
        }
        for (int j = 0; j < k; j++) {
            OpenMapRealVector column = getColumnVector(featureIndexes[j]);
            for (OpenIntToFloatHashMap.Iterator iterator = column.getEntries().iterator(); iterator.hasNext();) {
                iterator.advance();
                final int i = (int) iterator.key();
                final double value = iterator.value();
                FeatureNode fn = new FeatureNode((j + 1), value);
                data[i].add(fn);
            }
        }
        FeatureNode[][] out = new FeatureNode[getRowDimension()][];
        for (int i = 0; i < out.length; i++) {
            List<FeatureNode> list = data[i];
            out[i] = list.toArray(new FeatureNode[list.size()]);
        }
        return out;
    }

    public Instances getDatasetInstances(int[] featureIndexes, int k) {
        /**
         * Create attributes info.
         */
        FastVector attributes = new FastVector();
        for (int j = 0; j < k; j++) {
            attributes.addElement(new Attribute(String.valueOf(featureIndexes[j]), j));
        }
        FastVector attributes_class = new FastVector();
        attributes_class.addElement("c0");
        attributes_class.addElement("c1");
        attributes.addElement(new Attribute("y", attributes_class, attributes.size()));
        Instances dataRaw = new Instances("Instances", attributes, 0);
        dataRaw.setClassIndex(k);
        /**
         * Adding data.
         */
        double[] lables = getLables();
        FeatureNode[][] dataFN = getDatasetFeatureNode(featureIndexes, k);
        for (int i = 0; i < dataFN.length; i++) {
            FeatureNode[] instanceFN = dataFN[i];
            int[] tempIndices = new int[instanceFN.length];
            double[] tempValues = new double[instanceFN.length];
            for (int j = 0; j < instanceFN.length; j++) {
                tempIndices[j] = instanceFN[j].index - 1;
                tempValues[j] = instanceFN[j].value;
            }
            Instance instance = new SparseInstance(1, tempValues, tempIndices, dataRaw.numAttributes());
            instance.setDataset(dataRaw);
            if (lables[i] == -1) {
                instance.setValue(k, "c0");
            } else {
                instance.setValue(k, "c1");
            }
            dataRaw.add(instance);
        }
        return dataRaw;
    }

    /**
     * This method returns an array containing labels (-1 or 1).
     *
     * @return
     */
    public double[] getLables() {
        double[] out = new double[getRowDimension()];
        for (int i = 0; i < out.length; i++) {
            if (labels.getEntry(i) != 0) {
                out[i] = 1;
            } else {
                out[i] = -1;
            }
        }
        return out;
    }

    /**
     * This methods returns an array containing the stdev of each column.
     *
     * @return
     */
    public double[] getColumn_stdev() {
        return column_stdev;
    }

}
