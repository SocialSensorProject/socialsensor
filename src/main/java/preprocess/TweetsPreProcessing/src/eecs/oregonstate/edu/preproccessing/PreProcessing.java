/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eecs.oregonstate.edu.preproccessing;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import eecs.oregonstate.edu.tika.language.LanguageIdentifier;

/**
 * This class pre-process the raw dataset of Scott.
 * It will  print the data in following JSon format: created_at:, screen_name:, text:
 * NOTE: This class will delete the raw data after the preprocessing. 
 * So, copy the raw data first.
 * 
 * @author rbouadjenek
 */
public class PreProcessing {

    private int nbrDocs = 0;

    /**
     * This method parse a file line per line, following a JSon format. It also
     * write in the standard output using the JSon format.
     * @param file the file to be parsed
     * @return number of lines processed.
     */
    public int parseFile(File file) {
        int total = 0;
        JSONParser parser = new JSONParser();
        try {
            FileInputStream fstream = new FileInputStream(file);
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
                    if (str.startsWith("{\"created_at\":")) {
                        Object obj = parser.parse(str);
                        JSONObject jsonObject = (JSONObject) obj;
                        String text = (String) jsonObject.get("text");
                        text = text.replace("\n", "").replace("\r", "");
                        LanguageIdentifier identifier = new LanguageIdentifier(text);
                        String language = identifier.getLanguage();
                        if (language.equals("en")) {
                            total++;
                            JSONObject out=new JSONObject();
                            String date = (String) jsonObject.get("created_at");
                            out.put("created_at", date);
                            JSONObject userObject = (JSONObject) jsonObject.get("user");
//                        String id = (String) userObject.get("id_str");
                            String screen_name = (String) userObject.get("screen_name");
                            out.put("screen_name", screen_name);
                            out.put("text", text);
                            System.out.println(out.toJSONString());
                        }
                    }
                }
                return total;
            }
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            return 0;
        }
    }

    /**
     * Unzip the file
     *
     * @param gzipFile input zip file
     * @param unZippedFile output file
     */
    public void unZipIt(File gzipFile, File unZippedFile) {
        byte[] buffer = new byte[1024];
        try {
            FileOutputStream out;
            try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(gzipFile))) {
                out = new FileOutputStream(unZippedFile);
                int len;
                while ((len = gzis.read(buffer)) > 0) {
                    out.write(buffer, 0, len);
                }
            }
            out.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static class TextFilesFilter implements FileFilter {

        @Override
        public boolean accept(File path) {
            return path.getName().toLowerCase().endsWith(".gz") || path.getName().toLowerCase().endsWith(".bz2")
                    || path.getName().toLowerCase().endsWith(".lzo");
        }
    }

    /**
     * This method recursively process the directory folder
     *
     * @param dataDir directory of the data to be preprocessed
     * @throws java.lang.Exception
     */
    public void process(String dataDir) throws Exception {
        TextFilesFilter filter = new TextFilesFilter();
        File f = new File(dataDir);
        File[] listFiles = f.listFiles();
        for (File listFile : listFiles) {
            if (listFile.isDirectory()) {
                process(listFile.toString());
            } else {
                if (!listFile.isHidden() && listFile.exists() && listFile.canRead() && filter.accept(listFile)) {
                    nbrDocs++;
                    long start = System.currentTimeMillis();
                    File unZippedFile = new File(listFile.getAbsolutePath().replaceFirst("[.][^.]+$", ""));
                    unZipIt(listFile, unZippedFile);
                    int total = parseFile(unZippedFile);
                    long end = System.currentTimeMillis();
                    long millis = (end - start);
                    System.err.println("A total of " + total + " lines were parsed from the file "
                            + listFile.getName() + " in " + Functions.getTimer(millis) + ".");
                    unZippedFile.delete();
                }
            }
        }
    }

    /**
     * @return the number of documents pre-processed
     */
    public int getNbrDocs() {
        return nbrDocs;
    }

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        String dataDir;
        if (args.length == 0) {
            System.err.println("ERROR: incorrect parameters for eecs.oregonstate.edu.preproccessing.preproccessing! "
                    + "only " + args.length + " parameters :-(");
            System.err.println("Usage: java -jar PatentSearch.jar [options]");
            System.err.println("[options] have to be defined in the following order:");
            System.err.println("[-dataDir]: directory of the data");
        } else {
//            args = new String[1];
//            args[0] = "/Users/rbouadjenek/Documents/SocialMediaAnalysis/dataset/";
            dataDir = args[0];
            PreProcessing pre = new PreProcessing();
            long start = System.currentTimeMillis();
            pre.process(dataDir);
            long end = System.currentTimeMillis();
            long millis = (end - start);
            System.err.println("------------------------------------------------------------------------");
            System.err.println("There are " + pre.getNbrDocs() + " documents processed in " + Functions.getTimer(millis) + ".");
            System.err.println("------------------------------------------------------------------------");

        }

    }

}
