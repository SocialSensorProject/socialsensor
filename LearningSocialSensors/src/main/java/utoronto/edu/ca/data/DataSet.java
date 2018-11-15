/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.data;

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
import lirmm.inria.fr.math.linear.OpenLongToDoubleHashMap;
import lirmm.inria.fr.math.linear.OpenMapRealVector;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooLargeException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.exception.util.LocalizedFormats;
import org.apache.commons.math3.util.FastMath;

/**
 *
 * @author rbouadjenek
 */
public final class DataSet {

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
     * Number of rows of the matrix.
     */
//    private final int rows;
    /**
     * Number of columns of the matrix.
     */
    private final int columns;
    /**
     * Storage for (sparse) matrix elements.
     */
    private final List<OpenMapRealVector> entries;
    /**
     * Rank of features.
     */
    public List<ImmutablePair<Integer, Double>> feature_ranking;

    /**
     * File Storing data.
     */
    private final File file;

    public static DataSet readDataset(String file) throws FileNotFoundException, IOException {
        FileInputStream fstream;
        fstream = new FileInputStream(file);
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
        DataSet data = new DataSet(file, rows, columns, term_features, hashtag_features, mention_features, user_features, loc_feature);
        return data;
    }

    private DataSet(String file, int rowDimension, int columnDimension,
            int term_features, int hashtag_features, int mention_features, int user_features, int loc_feature) throws NotStrictlyPositiveException, NumberIsTooLargeException {
        long lRow = rowDimension;
        long lCol = columnDimension;
        if (lRow * lCol >= Long.MAX_VALUE) {
            throw new NumberIsTooLargeException(lRow * lCol, Long.MAX_VALUE, false);
        }
        this.entries = new ArrayList<>();
        this.columns = columnDimension;
        try (ProgressBar pb = new ProgressBar("Creating matrix", rowDimension)) {
            for (int i = 0; i < rowDimension; i++) {
                pb.step(); // step by 1
                pb.setExtraMessage(" in progress..."); // Set extra message to display at the end of the bar
                OpenMapRealVector row = new OpenMapRealVector(getColumnDimension());
                this.entries.add(row);
            }
        }
        System.out.println("reda1");
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
        System.out.println("reda2");
        try {
            fstream = new FileInputStream(file);
            System.out.println("reda3");
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
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

            for (int i = 0; i < getRowDimension(); i++) {
                OpenMapRealVector row = getRowVector(i);
                for (OpenLongToDoubleHashMap.Iterator iterator = row.getEntries().iterator(); iterator.hasNext();) {
                    iterator.advance();
                    final long j = iterator.key();
                    double v = rowNonZeroEntries.get(i);
                    rowNonZeroEntries.put(i, v + 1);
                    v = columnNonZeroEntries.get(j);
                    columnNonZeroEntries.put(j, v + 1);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the entries in row number {@code row} as a vector. Row indices
     * start at 0.
     *
     * @param row Row to be fetched.
     * @return a row vector.
     * @throws OutOfRangeException if the specified row index is invalid.
     */
    public OpenMapRealVector getRowVector(final int row) throws OutOfRangeException {
        checkRowIndex(row);
        return entries.get(row);
    }

    /**
     * This normalization divides only by stdev.
     */
    private void normalize() {
        double[] center = new double[getColumnDimension()];
        double[] scale = new double[getColumnDimension()];
        try (ProgressBar pb = new ProgressBar("Normalization", getRows().size() * 3)) {
            /**
             * Computing the mean of each column.
             */
            for (int i = 0; i < getRowDimension(); i++) {
                OpenMapRealVector row = getRowVector(i);
                for (OpenLongToDoubleHashMap.Iterator iterator = row.getEntries().iterator(); iterator.hasNext();) {
                    pb.step(); // step by 1
                    pb.setExtraMessage("Normalization in progress..."); // Set extra message to display at the end of the bar
                    iterator.advance();
                    final int j = (int) iterator.key();
                    final double value = iterator.value();
                    center[j] += value;
                }
            }

            for (int j = 0; j < center.length; j++) {
                center[j] = center[j] / getRowDimension();
            }
            /**
             * Computing stdev for each column.
             */
            for (int i = 0; i < getRowDimension(); i++) {
                OpenMapRealVector row = getRowVector(i);
                for (OpenLongToDoubleHashMap.Iterator iterator = row.getEntries().iterator(); iterator.hasNext();) {
                    pb.step(); // step by 1
                    pb.setExtraMessage("Normalization in progress..."); // Set extra message to display at the end of the bar
                    iterator.advance();
                    final double value = iterator.value();
                    final int j = (int) iterator.key();
                    scale[j] += Math.pow(value - center[j], 2);
                }
            }

            for (int j = 0; j < scale.length; j++) {
                int columnZero = getRowDimension() - getColumnNonZeroEntry(j); // number of entries in column j with zeros.
                scale[j] += (columnZero * Math.pow(center[j], 2));
                scale[j] = Math.sqrt(scale[j] / (getRowDimension() - 1));
            }
            /**
             * Dividing by stdev.
             */
            for (int i = 0; i < getRowDimension(); i++) {
                OpenMapRealVector row = getRowVector(i);
                for (OpenLongToDoubleHashMap.Iterator iterator = row.getEntries().iterator(); iterator.hasNext();) {
                    pb.step(); // step by 1
                    pb.setExtraMessage("Normalization in progress..."); // Set extra message to display at the end of the bar
                    iterator.advance();
                    final double value = iterator.value();
                    final int j = (int) iterator.key();
                    setEntry(i, j, value / scale[j]);
                }
            }
        }
    }

    public int getRowNonZeroEntry(int i) {
        return (int) rowNonZeroEntries.get(i);

    }

    public int getColumnNonZeroEntry(int j) {
        return (int) columnNonZeroEntries.get(j);

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

        for (int i = 0; i < getRowDimension(); i++) {
            OpenMapRealVector row = getRowVector(i);
            for (OpenLongToDoubleHashMap.Iterator iterator = row.getEntries().iterator(); iterator.hasNext();) {
                iterator.advance();
                final double value = iterator.value();
                final int j = (int) iterator.key();
                OpenMapRealVector column = out.get(j);
                column.setEntry(i, value);
            }
        }
        return out;
    }

    /**
     * Returns the number of rows of this matrix.
     *
     * @return the number of rows.
     */
    public int getRowDimension() {
        return entries.size();

    }

    /**
     * Returns the number of columns of this matrix.
     *
     * @return the number of columns.
     */
    public int getColumnDimension() {
        return columns;

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
        entries.get(row).setEntry(column, value);
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
     * Return the entries of the Matrix
     *
     * @return Entries of the matrix.
     */
    public List<OpenMapRealVector> getRows() {
        return entries;
    }

    /**
     * Rank features using mutual information.
     */
    public void rankFeatures() {
        feature_ranking = new ArrayList<>();
        List<OpenMapRealVector> cols = getColumnVectorsAsList();
        double size_posClass = labels.getEntries().size();
        double size_negClass = labels.getDimension() - size_posClass;
        int j = 0;
        try (ProgressBar pb = new ProgressBar("Ranking Features", getColumnDimension())) {
            for (OpenMapRealVector c : cols) {
                //Compute correlation between c and labels.
                pb.step(); // step by 1
                pb.setExtraMessage("Computing Mutual Information..."); // Set extra message to display at the end of the bar

                double posFeature = c.getSparsity();
                double negFeature = 1 - posFeature;

                double posFeature_posClass = 0;
                double posFeature_negClass = 0;

                for (OpenLongToDoubleHashMap.Iterator iterator = c.getEntries().iterator(); iterator.hasNext();) {
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
                outWritermention_features.println(pair.left + "\t" + pair.left + "\t" + pair.right);
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

}
