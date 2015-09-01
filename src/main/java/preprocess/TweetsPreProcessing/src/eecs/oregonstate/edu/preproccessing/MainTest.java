/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package preprocess.TweetsPreProcessing.src.eecs.oregonstate.edu.preproccessing;

import preprocess.TweetsPreProcessing.src.eecs.oregonstate.edu.tika.language.LanguageIdentifier;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author rbouadjenek
 */
public class MainTest {

    /**
     * @param args the command line arguments
     * @throws java.text.ParseException
     */
    public static void main(String[] args) throws java.text.ParseException {
        // TODO code application logic here
        DateFormat df1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzzz yyyy");
        DateFormat df2 = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");
        try {
            FileInputStream fstream = new FileInputStream(args[0]);
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String str;
                while ((str = br.readLine()) != null) {
                    if (str.startsWith("#")) {
                        continue;
                    }
                    if (str.trim().length() == 0) {
                        continue;
                    }
                    StringTokenizer st = new StringTokenizer(str, "\t");
                    String s[] = str.split("\t");
                    String el1 = s[0];
                    String el2 = s[1];
                    Date date1 = df1.parse(s[2]);
                    System.out.println(el1 + "\t" + el2 + "\t" + df2.format(date1));

                }
            }
        } catch (IOException e) {
        }

    }

}
