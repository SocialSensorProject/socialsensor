/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.util;

/**
 *
 * @author reda
 */
public class Misc {

    /**
     * This method converts a Double array to an int array.
     *
     * @param x The double data matrix.
     * @return
     */
    public static int[] double2IntArray(double[] x) {
        int[] out = new int[x.length];
        for (int i = 0; i < x.length; i++) {
            out[i] = (int) x[i];
        }
        return out;
    }
}
