/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca;

import java.text.DecimalFormat;
import org.apache.commons.math3.util.OpenIntToDoubleHashMap;

/**
 *
 * @author rbouadjenek
 */
public class NewMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        DecimalFormat formatter = new DecimalFormat("#,###");
        OpenIntToDoubleHashMap o = new OpenIntToDoubleHashMap(0f);
        for (int i = 0; i < 1000000000; i++) {
            o.put(i, i);
            if (i % 10000000 == 0) {
                System.out.println("i = " + formatter.format(i));
            }
        }

    }
}
