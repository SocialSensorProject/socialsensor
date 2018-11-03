/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lirmm.inria.fr.data;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lirmm.inria.fr.math.linear.BigSparseRealMatrix;
import lirmm.inria.fr.math.linear.OpenLongToDoubleHashMap;
import lirmm.inria.fr.math.linear.OpenMapRealVector;
import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.NumberIsTooLargeException;

/**
 *
 * @author rbouadjenek
 */
public final class DataMatrix extends BigSparseRealMatrix {

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
    private final OpenLongToDoubleHashMap labels = new OpenLongToDoubleHashMap(0.0);

    public DataMatrix(String file, int rowDimension, int columnDimension) throws NotStrictlyPositiveException, NumberIsTooLargeException {
//        super(5573, 100);
        super(rowDimension, columnDimension);
        loadMatrix(file, rowDimension, columnDimension);
        normalize();
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
                    Matcher m = Pattern.compile("\\s*(\\[[^\\]]*\\])|\\),[^\\]]*").matcher(str);
                    m.find();
                    String[] indices = m.group().replaceAll("\\[|\\]", "").split(",");
                    m.find();
                    String[] values = m.group().replaceAll("\\[|\\]", "").split(",");
                    m.find();
                    String label = m.group().replaceAll(",|\\)", "");
                    labels.put(i, Double.parseDouble(label));
                    for (int k = 0; k < indices.length; k++) {
                        int j = Integer.parseInt(indices[k]);
                        double val = Double.parseDouble(values[k]);
                        setEntry(i, j, val);
                    }
                    i++;
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
        for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
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

        for (OpenLongToDoubleHashMap.Iterator iterator = getEntries().iterator(); iterator.hasNext();) {
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
            setEntry(i, j, value / scale[j]);
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
}
