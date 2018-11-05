/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utoronto.edu.ca.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/**
 * This class transforms a TSV file into a CSV file.
 *
 * @author rbouadjenek
 */
public class TSV2CSV {

    /**
     * The tsv file.
     */
    protected String tsvfile;

    public TSV2CSV(String tsvfile) {
        this.tsvfile = tsvfile;
    }

    public void process() {
        try {
            FileInputStream fstream = new FileInputStream(tsvfile);
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String str;
                int i = -1;
                while ((str = br.readLine()) != null) {
                    i++;
                    if (str.trim().length() == 0) {
                        continue;
                    }
                    StringTokenizer st = new StringTokenizer(str, "\t");
                    String out_line = "";
                    while (true) {
                        String val = st.nextToken().trim();
                        if (i == 0) {
                            val = "\"" + val + "\"";
                        } else {
                            if (val.equals("NaN")) {
                                val = "";
                            }
                        }
                        if (st.hasMoreTokens()) {
                            out_line += val + ",";
                        } else {
                            out_line += val;
                            break;
                        }
                    }
                    System.err.println(i + " lines have been read.");
                    System.out.println(out_line);
                }
            }
        } catch (IOException | NumberFormatException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        String csvfile;
        if (args.length == 0) {
//            csvfile = "/Volumes/Macintosh HD/Users/mbouadjenek/Downloads/libsvm-3.21/subsample_dataset/quality_features_subsample.csv";
//            class_index = 221;
            throw new Exception("\nUsage: <input file> [<class index >] [<header | no header>]\n<class index >: The index of the class Y\n<header | no header>:  If there is a header line in the input file, specify skip header using -h.");
        } else {
            csvfile = args[0];
        }

        TSV2CSV tsv2csv = new TSV2CSV(csvfile);
        tsv2csv.process();
    }
}
