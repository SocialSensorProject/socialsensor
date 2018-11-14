/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca;

import java.io.IOException;
import java.util.Random;
import lirmm.inria.fr.math.linear.BigSparseRealMatrix;
import lirmm.inria.fr.math.linear.OpenLongToDoubleHashMap;

/**
 *
 * @author rbouadjenek
 */
public class NewMain2 {

    public static void main(String[] args) throws IOException, InterruptedException {
        // try-with-resource block
        Random rand = new Random();
//        
//        BigSparseRealMatrix m = new BigSparseRealMatrix(432734400, 1146514);
//        for (int k = 0; k < Integer.MAX_VALUE; k++) {
//            int i = rand.nextInt(432734400);
//            int j = rand.nextInt(1146514);
//            double v = rand.nextDouble();
//            m.setEntry(i, j, v);
//        }
        OpenLongToDoubleHashMap o = new OpenLongToDoubleHashMap(0.0);
        for (int k = 0; k < 120; k++) {
            System.out.println("k="+k);
            o.put(k, k);
        }
        
        System.out.println(o.size());

        for (OpenLongToDoubleHashMap.Iterator iterator = o.iterator(); iterator.hasNext();) {
            iterator.advance();
            final double value = iterator.value();
            final long key = iterator.key();
            System.out.println("key:" + key + ", value:" + value);
        }

    }

}
