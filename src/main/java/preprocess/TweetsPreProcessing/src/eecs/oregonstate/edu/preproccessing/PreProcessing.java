/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package preprocess.TweetsPreProcessing.src.eecs.oregonstate.edu.preproccessing;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.tika.language.LanguageIdentifier;
import org.json.JSONWriter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import preprocess.spark.ConfigRead;
//import preprocess.TweetsPreProcessing.src.eecs.oregonstate.edu.tika.language.LanguageIdentifier;

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
    private static ConfigRead configRead;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    /**
     * This method parse a file line per line, following a JSon format. It also
     * write in the standard output using the JSon format.
     * @param file the file to be parsed
     * @return number of lines processed.
     */
    public int parseFile(File file, String outputJsonPath) throws IOException {
        int total = 0;
        JSONParser parser = new JSONParser();
        FileOutputStream fileOS = new FileOutputStream(outputJsonPath + file.getName() + ".json.gz");
        GZIPOutputStream gzipOuputStream = new GZIPOutputStream(fileOS);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gzipOuputStream, "UTF-8"));
        final String emo_regex2 = "([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee])";//"\\p{InEmoticons}";
        Matcher matcher;
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
                            //TWEET INFORMATION
                            long tid = (long) jsonObject.get("id");
                            String date = (String) jsonObject.get("created_at");
                            String retweet_count = null, tweet_favorite_count = null;
                            if(jsonObject.get("retweet_count") != null)
                                retweet_count = jsonObject.get("retweet_count").toString();
                            if(jsonObject.get("favorite_count") != null)
                                tweet_favorite_count = jsonObject.get("favorite_count").toString();
                            out.put("id", tid);
                            out.put("retweet_count", retweet_count);
                            out.put("tweet_favorite_count", tweet_favorite_count);
                            out.put("created_at", date);
                            //matcher = Pattern.compile(emo_regex2).matcher(text);
                            //text = matcher.replaceAll("");
                            //out.put("text", text);
                            JSONObject userObject = (JSONObject) jsonObject.get("user");
                            // USER INFORMATION
                            String screen_name = (String) userObject.get("screen_name");
                            String user_location = (String) userObject.get("location");
                            String user_timeZone = (String) userObject.get("time_zone");
                            String favorite_count = userObject.get("favourites_count").toString();
                            String followers_count = userObject.get("followers_count").toString();
                            String friends_count = userObject.get("friends_count").toString();
                            String listed_count = userObject.get("listed_count").toString();
                            String statuses_count = userObject.get("statuses_count").toString();
                            out.put("screen_name", screen_name);
                            out.put("user_timezone", user_timeZone);
                            out.put("favorite_count", favorite_count);
                            out.put("followers_count", followers_count);
                            out.put("friends_count", friends_count);
                            out.put("listed_count", listed_count);
                            out.put("user_location", user_location);
                            out.put("statuses_count", statuses_count);
                            // PLACE INFROMATION
                            JSONObject jplace = (JSONObject) jsonObject.get("place");
                            if (jplace!=null) {
                                if (jplace.get("country_code")!=null)
                                    out.put("place_country_code", jplace.get("country_code").toString());
                                if (jplace.get("full_name")!=null)
                                    out.put("place_full_name", jplace.get("full_name").toString());
                                //if (jplace.get("id")!=null)
                                //        out.put("place_id", jplace.get("id").toString());
                                //if (jplace.get("url")!=null) if (jplace.get("url") != null) out.put("place_url", jplace.get("url").toString());
                            }else{
                                out.put("place_country_code", null);
                                out.put("place_full_name", null);
                            }
                            // GEO INFORMATION
                            if (jsonObject.get("coordinates") != null) {
                                JSONObject coords = ((JSONObject)jsonObject.get("coordinates"));
                                if ( coords.size() ==2 ) {
                                    if (coords.get(0)!=null)
                                        out.put("tweet_geo_lat", String.valueOf(Double.valueOf(coords.get(0).toString())));
                                    if (coords.get(1)!=null)
                                        out.put("tweet_geo_lng", String.valueOf(Double.valueOf(coords.get(1).toString())));
                                }
                            }else{
                                out.put("tweet_geo_lat", null);
                                out.put("tweet_geo_lng", null);
                            }
                            writer.write(out.toJSONString());
                            writer.write("\n");
                            System.out.println("Total: " + total);
                            //System.out.println(out.toJSONString());
                        }
                    }
                }
                writer.flush();
                writer.close();
                return total;
            }catch (IOException | IllegalArgumentException e){
                e.printStackTrace();
                return 0;
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
     * @throws Exception
     */
    public void process(String dataDir) throws Exception {
        TextFilesFilter filter = new TextFilesFilter();
        File f = new File(dataDir);
        File[] listFiles = f.listFiles();
        configRead = new ConfigRead();
        loadConfig();

        for (File listFile : listFiles) {
            if (listFile.isDirectory()) {
                process(listFile.toString());
            } else {
                if (!listFile.isHidden() && listFile.exists() && listFile.canRead() && filter.accept(listFile)) {
                    nbrDocs++;
                    long start = System.currentTimeMillis();
                    File unZippedFile = new File(listFile.getAbsolutePath().replaceFirst("[.][^.]+$", ""));
                    unZipIt(listFile, unZippedFile);
                    int total = parseFile(unZippedFile, configRead.getOutputJsonPath());
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
     * @throws Exception
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

    private boolean containsIllegalCharacters(String displayName)
    {
        final int nameLength = displayName.length();

        for (int i = 0; i < nameLength; i++)
        {
            final char hs = displayName.charAt(i);

            if (0xd800 <= hs && hs <= 0xdbff)
            {
                final char ls = displayName.charAt(i + 1);
                final int uc = ((hs - 0xd800) * 0x400) + (ls - 0xdc00) + 0x10000;

                if (0x1d000 <= uc && uc <= 0x1f77f)
                {
                    return true;
                }
            }
            else if (Character.isHighSurrogate(hs))
            {
                final char ls = displayName.charAt(i + 1);

                if (ls == 0x20e3)
                {
                    return true;
                }
            }
            else
            {
                // non surrogate
                if (0x2100 <= hs && hs <= 0x27ff)
                {
                    return true;
                }
                else if (0x2B05 <= hs && hs <= 0x2b07)
                {
                    return true;
                }
                else if (0x2934 <= hs && hs <= 0x2935)
                {
                    return true;
                }
                else if (0x3297 <= hs && hs <= 0x3299)
                {
                    return true;
                }
                else if (hs == 0xa9 || hs == 0xae || hs == 0x303d || hs == 0x3030 || hs == 0x2b55 || hs == 0x2b1c || hs == 0x2b1b || hs == 0x2b50)
                {
                    return true;
                }
            }
        }

        return false;
    }


}
