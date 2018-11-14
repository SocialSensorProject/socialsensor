/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.feature.analysis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import utoronto.edu.ca.data.DataSet;
import utoronto.edu.ca.util.Functions;

/**
 * Main class to initiate the feature analysis.
 *
 * @author rbouadjenek
 */
public class FeatureAnalysis {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws FileNotFoundException, IOException, Exception {
        // TODO code application logic here

        long start = System.currentTimeMillis();
        DataSet dm = DataSet.readDataset("dataset/test.csv");
        dm.printTopFeaturesByCategory();
        long end = System.currentTimeMillis();
        File f = new File(args[0]);
        System.out.println("-------------------------------------------------------------------------");
        long millis = (end - start);
        System.out.println("The processing of file " + f.getName() + "  took " + Functions.getTimer(millis) + ".");
        System.out.println("-------------------------------------------------------------------------");

    }

}
