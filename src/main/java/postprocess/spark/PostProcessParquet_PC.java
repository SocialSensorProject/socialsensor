package postprocess.spark;

import visualization.ScatterPlot;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import util.ConfigRead;
import util.TweetUtil;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PostProcessParquet_PC implements Serializable {
    private static String outputCSVPath;
    private static ConfigRead configRead;
    private static boolean findTopMiddle = true;
    public  static String[] topics;
    public  static String[] features = {"Location"};//, "From", "Mention", "Hashtag", "Term"};
    public  static String[] subAlgs = {"MI", "CP"};//"MI" "CE_Suvash", "JP"};//"CE"
    public static String ceName = "CE_Suvash";
    public static String clusterResultsPath = "/data/FeatureAnalysis/";
    public static int topFeatureNum = 1000;
    private static String scriptPath;
    private static TweetUtil tweetUtil;
    private static HashMap<Integer, String> indexFeatureName;
    private static int thresholdValue = 400;
    private static int numberOfLines = 10;
    private static String outPath2;
    private static String outPath3;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
        tweetUtil = new TweetUtil();
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        loadConfig();
        //numOfGroups = configRead.getNumOfGroups();
        topics = configRead.getGroupNames();
        scriptPath = configRead.getScriptPath();
        int itNum = configRead.getSensorEvalItNum();
        int hashtagNum = configRead.getSensorEvalHashtagNum();
        outputCSVPath = configRead.getOutputCSVPath();
        boolean local = configRead.isLocal();
        boolean calcNoZero = false;
        boolean convertParquet = false;
        boolean fixNumbers = false;
        boolean runScript = false;
        boolean makeScatterFiles = true;
        boolean cleanTerms = false;
        boolean buildLists = false;


        //if(local)
        //    clusterResultsPath = outputCSVPath;

        if(buildLists)
            getLists();
        if(makeScatterFiles)
            makeScatterFiles();
        if(cleanTerms)
            cleanTerms();

        if(convertParquet) {
            boolean readTopics = true;
            SparkConf sparkConfig;
            if (local) {
                outputCSVPath = "/data/FeatureAnalysis/";
                sparkConfig = new SparkConf().setAppName("PostProcessParquet_PC").setMaster("local[2]");
            } else
                sparkConfig = new SparkConf().setAppName("PostProcessParquet_PC");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
            SQLContext sqlContext = new SQLContext(sparkContext);
            if(readTopics)
                outputCSVPath = "/data/FeatureAnalysis/humancauseddisaster/";
            else
                outputCSVPath = "/data/MI2/";
            for(String ss : configRead.getGroupNames()) {
                outputCSVPath = "/data/FeatureAnalysis/"+ss+"/MI/";
                outPath2 = "/data/Baselines/"+ss+"/MI/";
                outPath3 = "/data/Baselines/"+ss+"/MILog/";
                tweetUtil.runStringCommand("mkdir " + "/data/Baselines/"+ss);
                tweetUtil.runStringCommand("mkdir " + "/data/Baselines/"+ss+"/MI");
                tweetUtil.runStringCommand("mkdir " + "/data/Baselines/"+ss+"/MILog/");
                File folder = new File(outputCSVPath);
                ArrayList<String> fileNames = listFilesForFolder(folder);
                DataFrame res;
                int ind = -1;
                int[] lineNumbers = new int[fileNames.size()];
            /*FileReader fileReaderA = new FileReader("ClusterResults/featureIndex.csv");
            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
            String line;
            indexFeatureName = new HashMap<>();
            while ((line = bufferedReaderA.readLine()) != null) {
                if (line.split(",").length < 3)
                    System.out.println(line);
                indexFeatureName.put(Integer.valueOf(line.split(",")[2]), line.split(",")[1]);
            }*/

                if (readTopics) {
                    // Read all parquet part by part results files and combine them into 1 csv file for each iteration per group
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "CP");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "MI");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "CP/From");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "CP/Hashtag");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "CP/Mention");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "CP/Term");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "CP/Location");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "MI/From");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "MI/Hashtag");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "MI/Mention");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "MI/Term");
                    tweetUtil.runStringCommand("mkdir " + outputCSVPath + "MI/Location");
                    for (String filename : fileNames) {
                        outputCSVPath = "/data/FeatureAnalysis/"+ss+"/MI/"+filename+"/";
                        ind++;
                        if (filename.equals("CP") || filename.equals("MI"))
                            continue;
                        /*if (filename.contains("_csv") || filename.contains(".csv"))
                            continue;*/
                        System.out.println(outputCSVPath + "/" + filename);
                        res = null;
                        //res = sqlContext.read().parquet(outputCSVPath + "/" + filename);
                        lineNumbers[ind] = readResults1(res, sqlContext, filename);
                    }
                } else {
                    for (String filename : fileNames) {
                        ind++;
                        if (filename.contains("_csv") || filename.contains(".csv"))
                            continue;
                        System.out.println(outputCSVPath + "/" + filename);
                        res = sqlContext.read().parquet(outputCSVPath + "/" + filename);
                        //lineNumbers[ind] = readResultsMI(res, sqlContext, filename);
                        lineNumbers[ind] = readResultsCounts(res, sqlContext, filename);
                    }
                }
                ind = 0;
                for (String filename : fileNames) {
                    System.out.println("FileName: " + filename + " #lines: " + lineNumbers[ind]);
                    ind++;
                }
            }
        }

        if(runScript) {
            // Combine all CSV files into one file for each group
            printForumla(itNum, hashtagNum);
            tweetUtil.runScript("cp " + scriptPath + "mergeFiles.sh " + outputCSVPath + "mergeFiles.sh");
            tweetUtil.runScript("chmod +x " + outputCSVPath + "mergeFiles.sh");
            tweetUtil.runScript("./" + outputCSVPath + "mergeFiles.sh");
        }
    }



    public static void writeHeader() throws IOException {
        double featureNum = 1006133;
        FileWriter fw = new FileWriter("ClusterResults/TestTrain_Arff/header.arff");
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write("@RELATION Name1\n");
        bw.write("@ATTRIBUTE topical integer\n");
        for(double i = 1; i <= featureNum; i++){
            bw.write("@ATTRIBUTE feature"+new BigDecimal(i).toPlainString()+" integer\n");
        }
        bw.close();
    }

    public static int readResults2(DataFrame results, JavaSparkContext sc, int index, String filename) throws IOException, InterruptedException {
        /*
        * testTrain_data
         |-- user: string (nullable = true)
         |-- term: string (nullable = true)
         |-- hashtag: string (nullable = true)
         |-- mentionee: string (nullable = true)
         |-- time: long (nullable = true)
         |-- tid: long (nullable = true)
         |-- topical: integer (nullable = true
        */
        String outputCSVPath2 = "ClusterResults/TestTrain/";

        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String topical = ""; String[] features;
                String out = "", time = "";
                if(row.get(6) != null) {
                    topical = row.get(6).toString();
                }
                if(row.get(0) != null) { // FROM Feature
                    out += row.get(0).toString() + " ";
                }
                if(row.get(1) != null && !row.get(1).toString().equals("null")) // TERM Feature
                    out += row.getString(1) + " ";
                if(row.get(2) != null && !row.get(2).toString().equals("null")) { // HASHTAG Feature
                    out += row.getString(2) + " ";
                }
                if(row.get(3) != null && !row.get(3).toString().equals("null")) { // MENTION Feature
                    out += row.getString(3) + " ";
                }
                if(row.get(4) != null && !row.get(4).toString().equals("null")) { // TIME Feature
                    //time += " " + format.format(row.getLong(4));
                    time = row.get(4).toString();
                }
                //if(row.get(5) != null && !row.get(5).toString().equals("null")) { // TWEETID Feature
                //    out += " " + row.getString(5);
                //}
                if(topical.equals("1"))
                    accumulator.add(1);
                if(out.length() > 0) {
                    out = out.substring(0, out.length() - 1);
                    features = out.split(" ");
                    Set<String> set = new HashSet<String>(features.length);
                    Collections.addAll(set, features);
                    double[] tmp = new double[set.size()];
                    int i = 0;
                    for (String s : set) {
                        tmp[i] = Double.valueOf(s);
                        i++;
                    }
                    Arrays.sort(tmp);
                    out = topical;
                    for (double st : tmp)
                        out += " " + new BigDecimal(st).toPlainString() + ":1";
                }else{
                    out = topical;
                }
                out += " " + time;

                /*out = "{0 " + topical;
                for(String st: features)
                    out += "," + st + " 1";
                out += "}";*/
                return out;
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputCSVPath2 + "out_" + filename + "_csv");
        System.out.println("Count: " + strRes.count());
        /*FileReader fileReaderA = new FileReader(outputCSVPath +"out_"+filename+"_csv/part-00000");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        String name = "";
        if(filename.contains("Term"))
            name = "Term";
        else if(filename.contains("Hashtag"))
            name = "Hashtag";
        else if(filename.contains("Mention"))
            name = "Mention";
        else if(filename.contains("From"))
            name = "From";
        String path = "";
        if(filename.contains("_1_"))
            path = "Disaster/";
        else if(filename.contains("_2_"))
            path = "Politics/";
        if(filename.contains("CondEntropy"))
            path += ceName+"/" + name + "/";
        else if(filename.contains("mutualEntropy"))
            path += "MI/" + name + "/";
        else if(filename.contains("ProbTweetTrueCond"))
            path += "CP/" + name + "/";
        else if(filename.contains("ProbTweetTrueContain"))
            path += "JP/" + name + "/";
        FileWriter fw = new FileWriter(clusterResultsPath + path + name+"1.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        while((line = bufferedReaderA.readLine()) != null){
            bw.write(line);
            bw.write("\n");
            numberOfLines++;
        }
        bw.close();
        bufferedReaderA.close();
        new File(outputCSVPath +"out_"+filename+"_csv/part-00000").delete();
        if(findTopMiddle) {
            //=================== GET TOP MIDDLE BOTTOM===========
            tweetUtil.runStringCommand("sed -n '1, " + configRead.getTopUserNum() + "p' " + outputCSVPath + "CSVOut_" + filename + ".csv >  " + outputCSVPath + "top10_CSVOut_" + filename + ".csv");
            tweetUtil.runStringCommand("sed -n '" + ((int) Math.floor(numberOfLines / 2) - (configRead.getTopUserNum() / 2)) + ", " + ((int) Math.floor(numberOfLines / 2) + (configRead.getTopUserNum() / 2 - 1)) + "p' " + outputCSVPath + "CSVOut_" + filename + ".csv >  " + outputCSVPath + "middle10_CSVOut_" + filename + ".csv");
            tweetUtil.runStringCommand("sed -n '" + (numberOfLines - (configRead.getTopUserNum() - 1)) + ", " + numberOfLines + "p' " + outputCSVPath + "CSVOut_" + filename + ".csv >  " + outputCSVPath + "tail10_CSVOut_" + filename + ".csv");
        }*/
        int numberOfLines = accumulator.value().intValue();
        return numberOfLines;
    }

    public static int readResultsCSV(String filename) throws IOException, InterruptedException {
        //if(!filename.startsWith("CSVOut"))
        //    return 0;
        filename = "/Volumes/SocSensor/Zahra/FeatureStatisticsRun_Sept1/ClusterResults/Disaster/MI/From/CSVOut_mutualEntropyTweetFromUser_1_parquet.csv";
        FileReader fileReaderA = new FileReader(filename);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(filename +"_2.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        String[] strs;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(",");
            if(line.split(",").length < 2) {
                System.out.println("LOOK: " + line);
                continue;
            }
            //if(strs[1].equals("0.0") || strs[1].equals("-0.0")) break;
            //bw.write(strs[0] + "," + new BigDecimal(strs[1]).toPlainString() + "," + strs[2]);
            bw.write(line);
            bw.write("\n");
            numberOfLines++;
        }
        bw.close();
        bufferedReaderA.close();
        if(findTopMiddle) {
            //=================== GET TOP MIDDLE BOTTOM===========
            tweetUtil.runStringCommand("sed -n '" + ((int) Math.floor(numberOfLines / 2) - 5) + ", " + ((int) Math.floor(numberOfLines / 2) + 4) + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "middle10_NoZero_" + filename);
            tweetUtil.runStringCommand("sed -n '" + (numberOfLines - 9) + ", " + numberOfLines + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "tail10_NoZero_" + filename);
        }
        System.out.println("Filename: " + filename + " #lines: " + numberOfLines);
        return numberOfLines;
    }

    public static int readResults1(DataFrame results, SQLContext sqlContext, String filename) throws IOException, InterruptedException {
        /**/
        String outputPath = outputCSVPath;
        String featureName = "";

        if(filename.contains("ProbTweetTrueCond")) {
            outputPath += "CP/";
            outPath2 += "CP/";
        }
        else if(filename.contains("mutualEntropy")) {
            outputPath += "MI/";
            outPath2 += "MI/";
        }

        if(filename.contains("Term"))
            featureName = "Term";
        if(filename.contains("Location"))
            featureName = "Location";
        else if(filename.contains("Hashtag"))
            featureName = "Hashtag";
        else if(filename.contains("ToUser") || filename.contains("Mention"))
            featureName = "Mention";
        else if(filename.contains("From"))
            featureName = "From";

        //outputPath += featureName + "/";


        /*JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0) + "," + new BigDecimal(row.get(1).toString()).toPlainString();
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputPath + featureName + "1_csv");
        numberOfLines = (int) results.count();
        tweetUtil.runStringCommand("mv " + outputPath + featureName + "1_csv/part-00000 " + outputPath + featureName + "1.csv");
        tweetUtil.runStringCommand("rm -rf " + outputPath + featureName + "1_csv");*/
        if(findTopMiddle) {
            //=================== GET TOP MIDDLE BOTTOM===========

            tweetUtil.runStringCommand("sed -n '1, " + 1000 + "p' " +outputPath + featureName + "1.csv >  " + outPath2 + "top1000_"+ featureName + ".csv");
            FileReader fileReaderA = new FileReader(outPath2 + "top1000_"+ featureName + ".csv");
            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
            String line;
            FileWriter fw = new FileWriter(outPath3 + "top1000_"+ featureName + ".csv");
            BufferedWriter bw = new BufferedWriter(fw);
            String [] strs; double val;
            while((line = bufferedReaderA.readLine()) != null) {
                strs = line.split(",");
                bw.write(strs[0] +","+ (double) (Math.log(Double.valueOf(strs[1])) / Math.log(2))+"\n");
            }
            bw.close();
            fileReaderA.close();
            //tweetUtil.runStringCommand("sed -n '" + ((int) Math.floor(numberOfLines / 2) - (configRead.getTopUserNum() / 2)) + ", " + ((int) Math.floor(numberOfLines / 2) + (configRead.getTopUserNum() / 2 - 1)) + "p' " + outputPath + featureName + "1.csv >  " + outputPath + "middle10_"+featureName + "1.csv");
        }
        return 0;
    }


    public static int readResultsMI(DataFrame results, SQLContext sqlContext, String filename) throws IOException, InterruptedException {
        /**/
        String outputPath = outputCSVPath;

        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                return row.toString();
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputPath + filename + "_csv");
        tweetUtil.runStringCommand("mv " + outputPath + filename + "_csv/part-00000 " + outputPath + filename + ".csv");
        tweetUtil.runStringCommand("rm -rf " + outputPath + filename + "_csv");
        return 0;
    }


    public static int readResultsCounts(DataFrame results, SQLContext sqlContext, String filename) throws IOException, InterruptedException {
        /**/
        final boolean locationFlag;
        if ((filename.contains("location")))
            locationFlag = true;
        else
            locationFlag = false;
        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                if(locationFlag)
                    return row.getString(0) + "," + row.get(1).toString();
                else
                    return row.getString(0) + "," + new BigDecimal(row.get(1).toString()).toPlainString();
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputCSVPath + "out_" + filename + "_csv");
        TweetUtil.runStringCommand("mv " + outputCSVPath + "out_" + filename + "_csv/part-00000 " + outputCSVPath + "CSVOut_" + filename + ".csv");
        tweetUtil.runStringCommand("rm -rf " +outputCSVPath + "out_" + filename + "_csv");
        return 0;
    }


    public static ArrayList<String> listFilesForFolder(final File folder) {
        ArrayList<String> fileNames = new ArrayList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                fileNames.add(fileEntry.getName());
                System.out.println(fileEntry.getName());
                //listFilesForFolder(fileEntry);
            } else {
                if(!fileEntry.getName().startsWith(".") && !fileEntry.getName().startsWith("_")) {
                    fileNames.add(fileEntry.getName());
                    System.out.println(folder.getPath() + "/" +  fileEntry.getName());
                }
            }
        }
        return fileNames;
    }





    public static void printForumla(int itNum, int hashtagNum){
        String str = "=AVERAGE(";
        for(int i = 1; i <= itNum; i++)
            str += "B" + String.valueOf((i-1)*hashtagNum + 1) + ",";
        str += ")";
        System.out.println(str);
    }


    public static int readResultsCSV2(String path, String filename) throws IOException, InterruptedException {
        //if(!filename.startsWith("From") || !filename.startsWith("Mention") || !filename.startsWith("Hashtag"))
        //    return 0;
        FileReader fileReaderA = new FileReader("ClusterResults/counts/name_numbers/CSVOut_term_tweetCount_parquet.csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter("ClusterResults/counts/name_numbers/CSVOut_term_tweetCount_parquet1.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        String [] strs; double val;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(",");
            bw.write(new BigDecimal(strs[1]).toPlainString() + "," + strs[0]);
            bw.write("\n");
            numberOfLines++;
        }
        bw.close();
        bufferedReaderA.close();
        System.out.println("Filename: " + path + filename + " #lines: " + numberOfLines);
        return numberOfLines;
    }

    public static void makeScatterFiles() throws IOException, InterruptedException {
        String outputBasePath = "/data/PlotFiles/";
        String hashtagUniqueCountBasePath = "/data/userFeatures/";
        String[] hashtagCounts = {"CSVOut_hashtag_tweetCount_parquet.csv", "CSVOut_hashtag_userCount_parquet.csv"};
        String[] mentionCounts = {"CSVOut_mention_tweetCount_parquet.csv"};
        String[] userCounts = {"CSVOut_user_favoriteCount_parquet.csv", "CSVOut_user_followersCount_parquet.csv", "CSVOut_user_friendsCount_parquet.csv","CSVOut_user_hashtagCount_parquet.csv", "CSVOut_user_tweetCount_parquet.csv"};//, "CSVOut_user_statusesCount_parquet.csv"};
        String[] termCounts = {"CSVOut_term_tweetCount_parquet.csv"};
        String[] locationCounts = {"CSVOut_location_userCount_parquet.csv"};
        String hashtagProbPath, hashtagUniqueCountPath, outputPath2, commonPath;
        HashMap<String, String[]> hashMap;
        List<ScatterPlot> objects;
        boolean flagCE, flag3;
        String[] listCounts;
        final String emo_regex2 = "([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee])";
        List<String[]> availLocs = new ArrayList<>();

        /*HashSet<String> st = new HashSet<String>();
        String [] usSet = new String[] {"Alabama","AL","Alaska","AK","Arizona","AZ","Arkansas","AR","California","CA","Colorado","CO","Connecticut","CT","Delaware","DE","Florida","FL","Georgia","GA","Hawaii","HI","Idaho","ID","Illinois","IL","Indiana","IN","Iowa","IA","Kansas","KS","Kentucky","KY","Louisiana","LA","Maine","ME","Maryland","MD","Massachusetts","MA","Michigan","MI","Minnesota","MN","Mississippi","MS","Missouri","MO","Montana","MT","Nebraska","NE","Nevada","NV","NewHampshire","NH","NewJersey","NJ","NewMexico","NM","NewYork","NY","NorthCarolina","NC","NorthDakota","ND","Ohio","OH","Oklahoma","OK","Oregon","OR","Pennsylvania","PA","RhodeIsland","RI","SouthCarolina","SC","SouthDakota","SD","Tennessee","TN","Texas","TX","Utah","UT","Vermont","VT","Virginia","VA","Washington","WA","WestVirginia","WV","Wisconsin","WI","Wyoming","WY",
                "ca","la", "indiana", "denver", "massachusetts", "southcarolina", "portland", "detroit", "minneapolis", "louisiana", "kansas", "orlando", "bc", "kentucky", "missouri", "nashville", "pennsylvania", "wisconsin", "ph", "oregon", "dc", "tennessee", "iowa","miami", "brooklyn", "maryland", "newjersey", "alabama", "virgina", "lasvegas", "philadelphia", "ky", "austin", "newyorkcity", "minnesota","michigan", "arizona", "colorado", "sandiego", "houston", "tn", "atlanta","boston", "nyc", "wa", "usa", "us", "tx", "texas", "california", "ny", "chicago", "losangeles", "fl", "ohio", "unitedstates", "florida", "il"};
        for(String sus : usSet)
            st.add(sus.toLowerCase());
        String [] usSet2 = new String[st.size()];
        int ius = 0;
        for(String state : st) {
            usSet2[ius] = state;
            System.out.println(state + ",");
            ius++;
        }

        availLocs.add(usSet2);*/    
        availLocs.add(new String[] {"newyorkcity","nyc","ny","ga","nv","wy","washington","minnesota","fl","florida","nashville","ok","virginia","arkansas","oh","iowa","wi","portland","virgina","california","newmexico","or","miami","delaware","wv","pa","nevada","wa","ph","nebraska","brooklyn","austin","wisconsin","indiana","oklahoma","losangeles","vt","usa","de","idaho","northcarolina","boston","ohio","md","minneapolis","ma","montana","arizona","denver","me","va","newjersey","mi","mn","mo","mt","sandiego","ms","louisiana","tennessee","us","pennsylvania","philadelphia","ut","newyork","michigan","tx","westvirginia","nc","utah","nd","ne","nh","vermont","nj","kansas","nm","oregon","tn","ca","rhodeisland","wyoming","atlanta","unitedstates","southcarolina","orlando","maryland","chicago","dc","ky","ks","alaska","sd","georgia","sc","ct","mississippi","co","la","lasvegas","illinois","texas","ri","alabama","hi","kentucky","id","southdakota","ia","northdakota","hawaii","newhampshire","az","maine","houston","colorado","ar","il","massachusetts","in","ak","al","missouri","connecticut","detroit"});
        availLocs.add(new String[] {"london", "uk", "england", "manchester", "unitedkingdom", "newcastle"});
        availLocs.add(new String[] {"indonesia"});
        availLocs.add(new String[] {"philippines"});
        availLocs.add(new String[] {"canada", "ca", "toronto", "ontario", "vancouver", "ottawa", "bc"});
        availLocs.add(new String[] {"india", "mumbai"});
        availLocs.add(new String[] {"southafrica", "capetown"});
        availLocs.add(new String[] {"nigeria"});
        availLocs.add(new String[] {"argentina"});
        availLocs.add(new String[] {"brasil"});
        availLocs.add(new String[] {"egypt"});
        availLocs.add(new String[] {"japan"});
        availLocs.add(new String[] {"mexico", "méxico"});
        availLocs.add(new String[] {"france", "paris"});
        availLocs.add(new String[] {"thailand", "kualalumpur"});
        availLocs.add(new String[] {"italy", "milan"});
        String[] locNames = {"US", "UK", "Indonesia", "Philippines", "Canada", "India", "South Africa", "Nigeria", "Argentina", "Brasil", "Egypt", "Japan", "Mexico", "France", "Thailand", "Italy"};
        String locName;
        boolean flagLoc;
        for(int dd = 0; dd < availLocs.size(); dd++) {
            locName = locNames[dd];
            for (String topic : topics) {
                //if(topic.equals("celebritydeath") || topic.equals("tennis") || topic.equals("space") || topic.equals("soccor")) continue;
                //tweetUtil.runStringCommand("mkdir " + outputBasePath + topic);
                for (String subAlg : subAlgs) {
                    tweetUtil.runStringCommand("mkdir " + outputBasePath + subAlg);
                    for (String feature : features) {
                        tweetUtil.runStringCommand("mkdir " + outputBasePath + subAlg + "/" + feature + "/");
                        switch (feature) {
                            case "Hashtag":
                                listCounts = hashtagCounts;
                                break;
                            case "Term":
                                listCounts = termCounts;
                                break;
                            case "Mention":
                                listCounts = mentionCounts;
                                break;
                            case "Location":
                                listCounts = locationCounts;
                                break;
                            default:
                                listCounts = userCounts;
                                break;
                        }

                        for (String countName : listCounts) {
                            if (countName.equals("CSVOut_user_statusesCount_parquet.csv"))
                                thresholdValue = 1000;
                            else if (countName.contains("tweetCount") || countName.contains("hashtagCount"))
                                thresholdValue = 20;
                            else
                                thresholdValue = 400;
                            tweetUtil.runStringCommand("mkdir " + outputBasePath + subAlg + "/" + feature + "/" + countName.split("_")[2] + "/");
                            commonPath = clusterResultsPath + topic + "/" + subAlg + "/";
                            hashtagProbPath = commonPath + feature + "/" + feature + "1.csv";
                            //if(topic.equals("socialissues") || topic.equals("irandeal")||topic.equals("lbgt")||topic.equals("naturaldisaster")||topic.equals("epidemics")) continue;
                            //    hashtagProbPath = "/data/FeatureStatisticsRun_Sept1/ClusterResults/Disaster/MI/From/From1_2.csv";
                            hashtagUniqueCountPath = hashtagUniqueCountBasePath + countName;
                            outputPath2 = outputBasePath + subAlg + "/" + feature + "/" + countName.split("_")[2] + "/" + topic + "-" + feature + "_" + countName.split("[._]")[2] + "_" + subAlg + "_" + locName + ".csv";
                            FileReader fileReaderA = new FileReader(hashtagProbPath);
                            FileReader fileReaderB = new FileReader(hashtagUniqueCountPath);
                            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
                            BufferedReader bufferedReaderB = new BufferedReader(fileReaderB);
                            hashMap = new HashMap<>();
                            String line;
                            //FileWriter fw = new FileWriter(outputPath);
                            //BufferedWriter bw = new BufferedWriter(fw);
                            FileWriter fw2 = new FileWriter(outputPath2);
                            BufferedWriter bw2 = new BufferedWriter(fw2);
                            int numberOfLines = 0;
                            String[] strs;
                            double val;
                            String[] value;
                            System.out.println(hashtagProbPath + " --- " + hashtagUniqueCountPath);
                            System.out.println(outputPath2);
                            while ((line = bufferedReaderB.readLine()) != null) {
                                if (line.split(",").length < 2)
                                    continue;
                                strs = new String[2];
                                if (countName.contains("hashtagCount") || (!countName.contains("mention_tweetCount") && countName.contains("tweetCount")) || countName.contains("hashtag_userCount") || countName.contains("hashtag_tweetCount")) {
                                    strs[0] = line.split(",")[0];
                                    strs[1] = line.split(",")[1].toLowerCase();
                                } else {
                                    strs[0] = line.split(",")[1];
                                    strs[1] = line.split(",")[0].toLowerCase();
                                }
                                int tmp = Double.valueOf(strs[0]).intValue();
                                //if(tmp < 20)
                                //    break;

                                String loc = strs[1];
                                Matcher matcher = Pattern.compile(emo_regex2).matcher(loc);
                                loc = matcher.replaceAll("").trim();
                                loc = loc.toLowerCase().replace(" ", "");
                                if(!Arrays.asList(availLocs.get(dd)).contains(loc))
                                    continue;
                                if (!hashMap.containsKey(strs[1])) {
                                    value = new String[2];
                                    value[0] = String.valueOf(tmp);// first string is the tweetCount
                                    value[1] = "";// second String is the probability
                                    hashMap.put(strs[1], value);
                                } else {
                                    value = hashMap.get(strs[1]);
                                    if (Integer.valueOf(value[0]) < tmp) {
                                        value[0] = String.valueOf(tmp);// first string is the tweetCount
                                        value[1] = "";// second String is the probability
                                        hashMap.put(strs[1], value);
                                    }
                                }
                            }
                            System.out.println("First file done");
                            flag3 = false;
                            while ((line = bufferedReaderA.readLine()) != null) { // Mention, 0.01, username
                                strs = line.split(",");
                                if (strs.length < 2)
                                    continue;
                                strs[0] = line.split(",")[1];
                                strs[1] = line.split(",")[0];
                                if (strs.length == 2 && !flag3) {
                                    if (hashMap.containsKey(strs[1])) {
                                        value = hashMap.get(strs[1]);
                                        value[1] = new BigDecimal(strs[0]).toPlainString();
                                        //if(strs[0].contains("E-9"))
                                        //    System.out.println(line);
                                        hashMap.put(strs[1], value);
                                    }
                                } else if (strs.length == 3) {
                                    flag3 = true;
                                    if (hashMap.containsKey(strs[2])) {
                                        value = hashMap.get(strs[2]);
                                        value[1] = new BigDecimal(strs[1]).toPlainString();
                                        hashMap.put(strs[2], value);
                                    }
                                }/*else {
                                if(!subAlg.equals("CE"))
                                    System.out.println("Something wrong " + strs[0] + " " + strs[1] + "  " + strs[2]);
                            }*/
                            }
                            System.out.println("Second file done");
                            objects = new ArrayList<ScatterPlot>();
                            flagCE = subAlg.equals("CE");
                            for (String key : hashMap.keySet()) {
                                if (hashMap.get(key)[1].equals(""))
                                    continue;
                                //objects.add(new ScatterPlot(Double.valueOf(hashMap.get(key)[1]), Double.valueOf(hashMap.get(key)[0]), key, flagCE));
                                objects.add(new ScatterPlot(Double.valueOf(hashMap.get(key)[1]), Double.valueOf(hashMap.get(key)[0]), key, flagCE));
                            }
                            Collections.sort(objects);
                            for (int i = 0; i < objects.size(); i++) {
                                if (i < 10)
                                    bw2.write(objects.get(i).getSecondDimCount() + "," + objects.get(i).getFeatureValue() + "," + objects.get(i).getFeatureKey() + "," + 1);
                                else if (i >= (objects.size() / 2 - 5) && i < (objects.size() / 2 + 5))
                                    bw2.write(objects.get(i).getSecondDimCount() + "," + objects.get(i).getFeatureValue() + "," + objects.get(i).getFeatureKey() + "," + 2);
                                else if (i >= objects.size() - 10)
                                    bw2.write(objects.get(i).getSecondDimCount() + "," + objects.get(i).getFeatureValue() + "," + objects.get(i).getFeatureKey() + "," + 3);
                                else
                                    bw2.write(objects.get(i).getSecondDimCount() + "," + objects.get(i).getFeatureValue() + "," + objects.get(i).getFeatureKey() + ",0");
                                bw2.write("\n");
                                //if(hashMap.get(key)[1].equals("")){
                                //if(!subAlg.equals("CE"))
                                //    System.out.println("Something wrong " + key + " "  + hashMap.get(key)[0]);
                                //else
                                //    continue;
                                //}
                                //bw.write(hashMap.get(key)[0] + "," + hashMap.get(key)[1]);
                                //bw.write("\n");
                            }
                            //bw.close();
                            bw2.close();
                            bufferedReaderA.close();
                            bufferedReaderB.close();
                        }
                    }
                }
            }
        }
    }

    public static void cleanTerms() throws IOException {
        FileReader fileReaderA = new FileReader(outputCSVPath+"/part-00000");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outputCSVPath +"cleanTerms.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int[] lineNum5_10_50_100 = new int[4];
        for(int i = 0; i < 4; i++)
            lineNum5_10_50_100[i] = 0;
        int numberOfLines = 0;
        String[] strs;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(",");
            strs[1] = new BigDecimal(strs[1]).toPlainString();
            numberOfLines++;
            if(numberOfLines < 571)
                continue;
            if(Double.valueOf(strs[1]) < 100) {
                lineNum5_10_50_100[3]++;
                if(Double.valueOf(line.split(",")[1]) < 50)
                    lineNum5_10_50_100[2]++;
                if(Double.valueOf(line.split(",")[1]) < 10)
                    lineNum5_10_50_100[1]++;
                if(Double.valueOf(line.split(",")[1]) < 5)
                    lineNum5_10_50_100[0]++;
                continue;
            }
            bw.write(strs[0] + "," + strs[1]);
            bw.write("\n");
        }
        bw.close();
        fileReaderA.close();
        System.out.println("Number of Terms with less than 5 occurences: " + lineNum5_10_50_100[0]);
        System.out.println("Number of Terms with less than 10 occurences: " + lineNum5_10_50_100[1]);
        System.out.println("Number of Terms with less than 50 occurences: " + lineNum5_10_50_100[2]);
        System.out.println("Number of Terms with less than 100 occurences: " + lineNum5_10_50_100[3]);
        System.out.println("Number of Terms retained after cleaning: " + (numberOfLines - lineNum5_10_50_100[3]));
    }


    public static void getNonZeroforCE() throws IOException, InterruptedException {
        int ind;
        System.out.println("BUILDING NON_ZERO FILES");

        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        for (String topic : topics) {
            ind = 0;
            for (String feature : features) {
                outputCSVPath = clusterResultsPath + topic + "/"+ceName+"/" + feature + "/";
                System.out.println(clusterResultsPath + topic + "/"+ceName+"/" + feature + "/" + feature + ".csv");
                fileReaderA = new FileReader(outputCSVPath +feature + "1.csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                String line;
                FileWriter fw = new FileWriter(outputCSVPath +feature + "_tmp.csv");
                BufferedWriter bw = new BufferedWriter(fw);
                int numberOfLines = 0;
                String[] strs;
                while((line = bufferedReaderA.readLine()) != null){
                    strs = line.split(",");
                    if(strs.length > 2) {
                        if (strs[1].equals("0.0") || strs[1].equals("-0.0"))
                            break;
                    }else {
                        if (strs[0].equals("0.0") || strs[0].equals("-0.0"))
                            break;
                    }
                    bw.write(line);
                    bw.write("\n");
                    numberOfLines++;
                }
                bufferedReaderA.close();
                bw.close();
                ind++;
                tweetUtil.runStringCommand("rm "+outputCSVPath + feature + "1.csv");
                tweetUtil.runStringCommand("mv "+outputCSVPath + feature + "_tmp.csv" + " " + outputCSVPath + feature + "1.csv");
            }
        }
    }

    public static void fixNumbers(String clusterResultPath, String topic, String subAlg, String feature) throws IOException, InterruptedException {
        String path = clusterResultsPath+ topic + "/" + subAlg + "/" +feature +"/";
        String fileName = feature+".csv";
        FileReader fileReaderA = new FileReader(path+fileName);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(path +"Tmp_"+fileName);
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        String [] strs;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(",");
            if(strs.length < 2) {
                if (line.contains("1.0132366607773573E-7"))
                    continue;
                continue;
            }
            if(strs.length > 2)
                bw.write(feature + "," + new BigDecimal(strs[1]).toPlainString() + "," + strs[2]);
            else
                bw.write(feature + "," + new BigDecimal(strs[0]).toPlainString() + "," + strs[1]);
            bw.write("\n");
            numberOfLines++;
        }
        bw.close();
        bufferedReaderA.close();
        tweetUtil.runStringCommand("rm " + path + fileName);
        tweetUtil.runStringCommand("mv " + path + "Tmp_" + fileName + " " + path + fileName);
        System.out.println("DONE");
    }

    public static void getLists() throws IOException, InterruptedException {
        String sortRandomly;
        getNonZeroforCE(); // This is from the original result file outputted from cluster in the format: value1, feature1
        for (String topic : topics) {
            for(String subAlg: subAlgs) {
                for(String feature : features) {
                    if(subAlg.equals("CE") || subAlg.equals("CE_Suvash")) {
                        tweetUtil.runStringCommand("cd " + clusterResultsPath + topic + "/" + subAlg + "/; tail -" + topFeatureNum + " " + feature + "/" + feature + "1.csv > " + feature + "/" + feature + ".csv;");
                    }else
                        tweetUtil.runStringCommand("cd " + clusterResultsPath + topic + "/" + subAlg + "/; sed -n '1,"+topFeatureNum+"p' " + feature + "/" + feature + "1.csv > " + feature + "/" + feature + ".csv;");
                    // Fix scientific numbers and add featureName column to the beginning
                    fixNumbers(clusterResultsPath, topic, subAlg, feature);
                    // SORT the CE lowest to highest
                    if(subAlg.equals("CE") || subAlg.equals("CE_Suvash")) {
                        tweetUtil.runStringCommand("cd " + clusterResultsPath + topic + "/" + subAlg + "/; sort --field-separator=',' -n -k2,2 " + feature + "/" + feature + ".csv > " + feature + "/" + feature + "_tmp.csv;");
                        tweetUtil.runStringCommand("rm " +  clusterResultsPath + topic + "/" + subAlg + "/" + feature + "/" + feature + ".csv");
                        tweetUtil.runStringCommand("mv " + clusterResultsPath + topic + "/" + subAlg + "/" + feature + "/" + feature + "_tmp.csv  " + clusterResultsPath + topic + "/" + subAlg + "/" + feature + "/" + feature + ".csv");
                    }
                }
                String command = "cd " + clusterResultsPath + topic + "/" + subAlg + "/; cat ";
                for(String feature: features)
                    command += feature + "/" + feature + ".csv ";
                command += " > mixed.csv";

                tweetUtil.runStringCommand(command);

                if(checkEquality(clusterResultsPath + topic + "/" + subAlg + "/mixed.csv")) {
                    sortRandomly = "";
                    for(String feature: features)
                        sortRandomly += "head -"+topFeatureNum + " " + clusterResultsPath + topic + "/" + subAlg +"/" + feature + "/" + feature + ".csv > " + feature + "2.csv; ";
                    sortRandomly = "cat ";
                    for(String feature: features)
                        sortRandomly += clusterResultsPath + topic + "/" + subAlg + "/" + feature +"/" + feature + "2.csv ";
                    sortRandomly += "; rm ";
                    for(String feature: features)
                        sortRandomly += clusterResultsPath + topic + "/" + subAlg + "/" + feature +"/" + feature + "2.csv ";
                    tweetUtil.runStringCommand(sortRandomly);
                }
                if(subAlg.equals("CE") || subAlg.equals("CE_Suvash"))
                    tweetUtil.runStringCommand("cd " + clusterResultsPath + topic + "/" + subAlg + "/; sort --field-separator=',' -n -k2,2 mixed.csv  > mixed1.csv;  sed -n '1,"+topFeatureNum+"p' mixed1.csv > mixed.csv; rm mixed1.csv;");
                else
                    tweetUtil.runStringCommand("cd " + clusterResultsPath + topic + "/" + subAlg + "/; sort --field-separator=',' -rn -k2,2 mixed.csv  > mixed1.csv;  sed -n '1,"+topFeatureNum+"p' mixed1.csv > mixed.csv; rm mixed1.csv;");
            }
        }
        //writeHeader();
    }

    private static boolean checkEquality(String fileName) throws IOException {
        FileReader fileReaderA = new FileReader(fileName);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        String [] strs;
        double value = Double.valueOf(bufferedReaderA.readLine().split(",")[1]);
        while((line = bufferedReaderA.readLine()) != null){
            if(Double.valueOf(line.split(",")[1]) != value) {
                bufferedReaderA.close();
                return false;
            }
        }
        bufferedReaderA.close();
        return true;
    }
}














