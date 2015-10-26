package postprocess.spark;

import machinelearning.ScatterPlot;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import preprocess.spark.ConfigRead;
import util.TweetUtil;
import util.ValueComparator;

import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PostProcessParquet2 implements Serializable {
    private static String outputCSVPath;
    private static ConfigRead configRead;
    private static boolean findTopMiddle = false;
    public  static String[] topics = {"Politics", "Disaster"};
    public  static String[] features = {"Mention", "From"};//, "Hashtag", "Term", "UserFeatures"};
    public  static String[] subAlgs = { "MI", "CP"};//, "CE_Suvash","JP",};//"CE"
    public static String ceName = "CE_Suvash";
    public static String clusterResultsPath = "/Volumes/SocSensor/Zahra/SocialSensor/FeatureStatisticsRun_Sept1/ClusterResults/";
    public static int topFeatureNum = 1000;
    private static String scriptPath;
    private static TweetUtil tweetUtil = new TweetUtil();
    private static Map<String, Long> hashtagMap = new HashMap<>();
    final static int groupNum = 1;
    private static BufferedWriter bwTrec;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String args[]) throws IOException, InterruptedException {

        loadConfig();
        scriptPath = configRead.getScriptPath();
        int itNum = configRead.getSensorEvalItNum();
        int hashtagNum = configRead.getSensorEvalHashtagNum();
        outputCSVPath = configRead.getOutputCSVPath();
        boolean local = configRead.isLocal();
        boolean calcNoZero = false;
        boolean convertParquet = false;
        boolean fixNumbers = false;
        boolean runScript = false;
        boolean makeScatterFiles = false;
        boolean cleanTerms = false;
        boolean buildLists = false;
        boolean readBaselineResults = false;
        boolean readLearningResults = true;

        //if(local)
        //    clusterResultsPath = outputCSVPath;

        if(buildLists)
            getLists();
        if(makeScatterFiles)
            makeScatterFiles();
        if(cleanTerms)
            cleanTerms();

        if(readBaselineResults){
            readBaselineResults(local);
        }

        if(readLearningResults){
            readLearningResults(local);
        }

        if(convertParquet) {
            boolean readTestTrain = false;
            String topic = configRead.getGroupNames()[groupNum-1];
            SparkConf sparkConfig;
            if (local) {
                tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV");
                tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV/" + topic);

                ///outputCSVPath = "ClusterResults/BaselinesRes/"+topic+"/";
                FileReader fileReaderA;
                BufferedReader bufferedReaderA;
                fileReaderA = new FileReader("Data/Learning/Topics/featureData/featureIndex_bu.csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                hashtagMap = new HashMap<>();
                String line;
                int featCount = 0;
                String[] feats;
                while ((line = bufferedReaderA.readLine()) != null) {
                    featCount++;
                    feats = line.split(",");
                    if(featCount < 361789)
                        hashtagMap.put("from:"+feats[0], Long.valueOf(feats[1]));
                    else if(featCount < 676753)
                        hashtagMap.put("term:"+feats[0], Long.valueOf(feats[1]));
                    else if(featCount < 864339)
                        hashtagMap.put("hashtag:"+feats[0], Long.valueOf(feats[1]));
                    else if(featCount < 864339+244478)
                        hashtagMap.put("mention:"+feats[0], Long.valueOf(feats[1]));
                    else
                        hashtagMap.put("location:"+feats[0], Long.valueOf(feats[1]));
                    //hashtagMap.put(line.split(",")[0], Long.valueOf(line.split(",")[1]));
                }
                bufferedReaderA.close();
                sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
            } else
                sparkConfig = new SparkConf().setAppName("PostProcessParquet");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
            SQLContext sqlContext = new SQLContext(sparkContext);

            // Read all parquet part by part results files and combine them into 1 csv file for each iteration per group
            File folder1 = new File(outputCSVPath);
            ArrayList<String> fileNames1 = listFilesForFolder(folder1);
            for (String filename1 : fileNames1) {
                tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV/" + topic +"/"+filename1);
                File folder = new File(outputCSVPath+"/"+filename1);
                ArrayList<String> fileNames = listFilesForFolder(folder);
                DataFrame res;
                int ind = -1;
                int[] lineNumbers = new int[fileNames.size()];
                for (String filename : fileNames) {
                    ind++;
                    if (filename.contains(".csv") || filename.contains("_csv") || filename.equals("out") || filename.contains("trecout_all_"))
                        continue;
                    System.out.println(outputCSVPath + "/" + filename1+"/" + filename);
                    res = sqlContext.read().parquet(outputCSVPath + "/" + filename1 + "/" + filename);
                    if (readTestTrain) {
                        if (filename.contains("strings"))// && !filename.contains("_4_")&& !filename.contains("_5_")&& !filename.contains("_6_"))
                            lineNumbers[ind] = readResults2Strings(res, sparkContext, ind, filename);
                        else {
                            lineNumbers[ind] = readResults2(res, sparkContext, ind, filename);
                            lineNumbers[ind] = readResults2Index(res, sparkContext, ind, filename);
                            String outputCSVPath2 = "ClusterResults/TestTrainDataCSV/";
                            FileReader fileReaderA = new FileReader(outputCSVPath2 + "out_" + filename + "_index.csv");
                            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
                            FileReader fileReaderB = new FileReader(outputCSVPath2 + "out_" + filename + ".csv");
                            BufferedReader bufferedReaderB = new BufferedReader(fileReaderB);
                            FileWriter fw = new FileWriter(outputCSVPath2 + "out_" + filename + "_all.csv");
                            BufferedWriter bw = new BufferedWriter(fw);
                            String line = "", line2 = "";
                            while ((line = bufferedReaderA.readLine()) != null) {
                                line2 = bufferedReaderB.readLine();
                                bw.write(line + "\n" + line2 + "\n");
                            }
                            bw.close();
                            bufferedReaderA.close();
                            bufferedReaderB.close();
                        }
                    }
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

    private static void readBaselineResults(boolean local) throws IOException, InterruptedException {
        boolean readTrecResults = false;
        String topic = configRead.getGroupNames()[groupNum-1];
        SparkConf sparkConfig;
        if (local) {
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV");
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV/" + topic);
            outputCSVPath = "ClusterResults/BaselinesRes_Tmp/"+topic+"/";
            if(readTrecResults) {
                FileWriter fwTrec = new FileWriter("ClusterResults/BaselinesResCSV/" + topic + "/" + "trecout_all_" + topic + ".csv");
                bwTrec = new BufferedWriter(fwTrec);
            }
            sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
        } else
            sparkConfig = new SparkConf().setAppName("PostProcessParquet");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        File folder1 = new File(outputCSVPath);
        ArrayList<String> fileNames1 = listFilesForFolder(folder1);
        for (String filename1 : fileNames1) {
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV/" + topic +"/"+filename1);
            File folder = new File(outputCSVPath+"/"+filename1);
            ArrayList<String> fileNames = listFilesForFolder(folder);
            DataFrame res;
            int ind = -1;
            int[] lineNumbers = new int[fileNames.size()];
            for (String filename : fileNames) {
                ind++;
                if (filename.contains(".csv") || filename.contains("_csv") || filename.equals("out"))
                    continue;
                System.out.println(outputCSVPath + "/" + filename1+"/" + filename);
                if(readTrecResults){
                    readTrecResults("ClusterResults/BaselinesResCSV/"+topic+"/" + filename1+"/",filename, bwTrec, topic, filename1);
                }else {
                    res = sqlContext.read().parquet(outputCSVPath + "/" + filename1 + "/" + filename);
                    System.out.println("LOOK: ");
                    res.printSchema();
                    lineNumbers[ind] = readResults1(res, sqlContext, ind, filename, "ClusterResults/BaselinesResCSV/" + topic + "/" + filename1 + "/");
                }
            }
        }
        if(readTrecResults)
            bwTrec.close();
    }
    private static void readLearningResults(boolean local) throws IOException, InterruptedException {
        boolean readTrecResults = false;
        String topic = configRead.getGroupNames()[groupNum-1];
        SparkConf sparkConfig;
        if (local) {
            //tweetUtil.runStringCommand("mkdir " + "ClusterResults/LearningRes");
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/LearningResCSV/" + topic);
            outputCSVPath = "ClusterResults/BaselinesRes_Tmp/Learning/Topics/"+topic+"/";
            if(readTrecResults) {
                FileWriter fwTrec = new FileWriter("ClusterResults/LearningResCSV/" + topic + "/" + "trecout_all_" + topic + ".csv");
                bwTrec = new BufferedWriter(fwTrec);
            }
            sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
        } else
            sparkConfig = new SparkConf().setAppName("PostProcessParquet");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        File folder1 = new File(outputCSVPath);
        ArrayList<String> fileNames1 = listFilesForFolder(folder1);
        for (String filename1 : fileNames1) {
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/LearningResCSV/" + topic +"/"+filename1);
            DataFrame res;
            int ind = -1;
            int[] lineNumbers = new int[fileNames1.size()];
            ind++;

            System.out.println(outputCSVPath + "/" + filename1+"/" + filename1);
            if(readTrecResults){
                readTrecResults("ClusterResults/LearningResCSV/"+topic+"/",filename1, bwTrec, topic, filename1);
            }else {
                if(!filename1.contains("_all"))
                    continue;
                res = sqlContext.read().parquet(outputCSVPath + "/" + filename1);
                System.out.println("LOOK: ");
                res.printSchema();
                lineNumbers[ind] = readResults1(res, sqlContext, ind, filename1, "ClusterResults/LearningResCSV/" + topic + "/");
            }

        }
        if(readTrecResults)
            bwTrec.close();
    }

    public static void readHashtagSetDateResuts(String filename) throws IOException {
        FileReader fileReaderA = new FileReader(clusterResultsPath + filename);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outputCSVPath + filename +".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        String[] strs;
        Map<String, Long> hashtagDate = new HashMap<>();
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(",");
        /*    if(hashtagDate.containsKey(strs[1])){
                if(hashtagDate.get(strs[1]) < strs[2])
                    hashtagDate.put(strs[1], strs[2]);
            }*/
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
        * root
         |-- username: string (nullable = true)
         |-- term: string (nullable = true)
         |-- hashtag: string (nullable = true)
         |-- mentionee: string (nullable = true)
         |-- location: string (nullable = true)
         |-- time: long (nullable = true)
         |-- tid: long (nullable = true)
         |-- topical: integer (nullable = true)

        */
        String outputCSVPath2 = "ClusterResults/TestTrainDataCSV/";

        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String topical = "";
                String[] features;
                String out = "", time = "", tid = "";
                if (row.length() > 7 && row.get(7) != null) {
                    topical = row.get(7).toString();
                }
                if (row.get(0) != null) { // FROM Feature
                    out += "from:" + row.get(0).toString() + " ";
                }
                if (row.get(1) != null && !row.get(1).toString().equals("null")) // TERM Feature
                    for(String ss: row.getString(1).split(" "))
                        out += "term:" + ss + " ";
                if (row.get(2) != null && !row.get(2).toString().equals("null")) { // HASHTAG Feature
                    for(String ss: row.getString(2).split(" "))
                        out += "hashtag:" + ss + " ";
                }
                if (row.get(3) != null && !row.get(3).toString().equals("null")) { // MENTION Feature
                    for(String ss: row.getString(3).split(" "))
                        out += "mention:" + ss + " ";
                }
                if (row.get(4) != null && !row.get(4).toString().equals("null")) { // LOCATION Feature
                    out += "location:" + row.getString(4) + " ";
                }
                if (row.get(5) != null && !row.get(5).toString().equals("null")) { // TIME Feature
                    //time += " " + format.format(row.getLong(4));
                    time = row.get(5).toString();
                }
                if (row.get(6) != null && !row.get(6).toString().equals("null")) { // TID Feature
                    //time += " " + format.format(row.getLong(4));
                    tid = row.get(6).toString();
                }

                if (row.length() == 7)
                    return out + " " + time;
                if (topical.equals("1"))
                    accumulator.add(1);
                if (out.length() > 0) {
                    out = out.substring(0, out.length() - 1);
                    /*features = out.split(" ");
                    Set<String> set = new HashSet<String>(features.length);
                    Collections.addAll(set, features);
                    long[] tmp = new long[set.size()];
                    int i = 0;
                    for (String s : set) {
                        tmp[i] = Long.valueOf(s);
                        i++;
                    }
                    Arrays.sort(tmp);
                    out = topical;
                    for (double st : tmp)
                        out += " " + new BigDecimal(st).toPlainString() + ":1";*/
                } else {
                    out = topical;
                }
                out += " " + time + " " + tid;

                /*out = "{0 " + topical;
                for(String st: features)
                    out += "," + st + " 1";
                out += "}";*/
                return topical + " " + out;
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputCSVPath2 + "out_" + filename + "_csv");
        System.out.println("Count: " + strRes.count());
        tweetUtil.runStringCommand("mv " + outputCSVPath2 + "out_" + filename + "_csv/part-00000 " + outputCSVPath2 + "out_" + filename + ".csv");
        tweetUtil.runStringCommand("rm -rf " + outputCSVPath2 + "out_" + filename + "_csv");
        int numberOfLines = accumulator.value().intValue();
        return numberOfLines;
    }

    public static int readResults2Index(DataFrame results, JavaSparkContext sc, int index, String filename) throws IOException, InterruptedException {
        /*
        * root
         |-- username: string (nullable = true)
         |-- term: string (nullable = true)
         |-- hashtag: string (nullable = true)
         |-- mentionee: string (nullable = true)
         |-- location: string (nullable = true)
         |-- time: long (nullable = true)
         |-- tid: long (nullable = true)
         |-- topical: integer (nullable = true)

        */
        final String emo_regex2 = "\\([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee]\\)";//"\\p{InEmoticons}";
        String outputCSVPath2 = "ClusterResults/TestTrainDataCSV/";

        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String topical = ""; String[] features;
                String out = "", time = "", tid = "", out2 = "";
                if(row.length() > 7 && row.get(7) != null) {
                    topical = row.get(7).toString();
                }
                if(row.get(0) != null) { // FROM Feature
                    out += "from:"+row.get(0).toString() + " ";
                }
                if(row.get(1) != null && !row.get(1).toString().equals("null")) // TERM Feature
                    for(String ss: row.getString(1).split(" "))
                        out += "term:" + ss + " ";
                if(row.get(2) != null && !row.get(2).toString().equals("null")) { // HASHTAG Feature
                    for(String ss: row.getString(2).split(" "))
                        out += "hashtag:" + ss + " ";
                }
                if(row.get(3) != null && !row.get(3).toString().equals("null")) { // MENTION Feature
                    for(String ss: row.getString(3).split(" "))
                        out += "mention:" + ss + " ";
                }
                if(row.get(4) != null && !row.get(4).toString().equals("null")) { // LOCATION Feature
                    String loc = row.getString(4);
                    Matcher matcher = Pattern.compile(emo_regex2).matcher(loc);
                    loc = matcher.replaceAll("").trim();
                    loc = loc.toLowerCase().replace(" ", "");
                    out += "location:"+loc+ " ";
                }
                if(row.get(5) != null && !row.get(5).toString().equals("null")) { // TIME Feature
                    //time += " " + format.format(row.getLong(4));
                    time = row.get(5).toString();
                }
                if(row.get(6) != null && !row.get(6).toString().equals("null")) { // TID Feature
                    //time += " " + format.format(row.getLong(4));
                    tid = row.get(6).toString();
                }

                if(topical.equals("1"))
                    accumulator.add(1);
                if(out.length() > 0) {
                    out = out.substring(0, out.length() - 1);
                    features = out.split(" ");
                    Set<String> set = new HashSet<String>(features.length);
                    Collections.addAll(set, features);
                    List<Long> tmp = new ArrayList<Long>();
                    int i = 0;
                    for (String s : set) {
                        if(hashtagMap.get(s) == null)
                            continue;
                        tmp.add(hashtagMap.get(s));
                    }
                    Collections.sort(tmp);
                    out = topical;
                    for (long st : tmp)
                        out += " " + new BigDecimal(st).toPlainString() + ":1";
                }else{
                    out = topical;
                }
                out += " " + time + " " + tid;

                /*out = "{0 " + topical;
                for(String st: features)
                    out += "," + st + " 1";
                out += "}";*/
                return out;
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputCSVPath2 + "out_" + filename + "_index_csv");
        System.out.println("Count: " + strRes.count());
        tweetUtil.runStringCommand("mv " + outputCSVPath2 + "out_" + filename + "_index_csv/part-00000 " + outputCSVPath2 + "out_" + filename + "_index.csv");
        tweetUtil.runStringCommand("rm -rf " + outputCSVPath2 + "out_" + filename + "_index_csv");
        int numberOfLines = accumulator.value().intValue();
        return numberOfLines;
    }

    public static int readResults2Strings(DataFrame results, JavaSparkContext sc, int index, String filename) throws IOException, InterruptedException {
        /*
        * root
         |-- tid: long (nullable = true)
         |-- user: string (nullable = true)
         |-- location: string (nullable = true)
         |-- time: long (nullable = true)
         |-- hashtagGrouped: string (nullable = true)
         |-- mentionee: string (nullable = true)
         |-- term: string (nullable = true)
        */
        String outputCSVPath2 = "ClusterResults/TestTrainDataCSV/";

        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String topical = ""; String[] features;
                String out = "", time = "", tid = "";
                if(row.get(0) != null) { // TID Feature
                    tid = String.valueOf(row.getLong(0));
                }
                if(row.get(1) != null && !row.get(1).toString().equals("null")) // USER Feature
                    out += row.getString(1) + " ";
                if(row.get(2) != null && !row.get(2).toString().equals("null")) { // LOCATION Feature
                    out += row.getString(2) + " ";
                }
                if(row.get(3) != null && !row.get(3).toString().equals("null")) { // TIME Feature
                    time = row.get(3).toString();
                }
                if(row.get(4) != null && !row.get(4).toString().equals("null")) { // HASHTAG Feature
                    out += row.getString(4) + " ";
                }
                if(row.get(5) != null && !row.get(5).toString().equals("null")) { // MENTION Feature
                    out += row.getString(5) + " ";
                }
                if(row.get(6) != null && !row.get(6).toString().equals("null")) { // TERM Feature
                    out += row.getString(6) + " ";
                }
                if(out.length() > 0)
                    out = out.substring(0, out.length() - 1);
                return tid + " " + out;
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputCSVPath2 + "out_" + filename + "_csv");
        System.out.println("Count: " + strRes.count());
        tweetUtil.runStringCommand("mv " + outputCSVPath2 + "out_" + filename + "_csv/part-00000 " + outputCSVPath2 + "out_" + filename + ".csv");
        tweetUtil.runStringCommand("rm -rf " + outputCSVPath2 + "out_" + filename + "_csv");
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

    public static int readResults1(DataFrame results, SQLContext sqlContext, int index, final String filename, String outPath) throws IOException, InterruptedException {
        /**/
        boolean flagFromTmp = false;
        final boolean flagLearningRes = false;
        if(filename.contains("From"))
            flagFromTmp = true;
        final boolean flagFrom = flagFromTmp;
        /*JavaRDD strRes1 = results.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                return row.toString();
            }
        });
        strRes1.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_csv");
        tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_csv/part-00000" + " " + outPath + "out_" + filename + ".csv");
        tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename  + "_csv");*/
        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                String s = "Q0";
                if(flagFrom)
                    return groupNum + " " + s + " " + row.getLong(2) + " " + row.getInt(4);
                else if (flagLearningRes)
                    return groupNum + " " + s + " " + row.getLong(0) + " " + row.getInt(2);
                else
                    return groupNum + " " + s + " " + row.getLong(0) + " " + row.getInt(2);
            }
        });
        strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qrel" + "_csv");
        tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qrel" + ".csv");
        tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename + "_qrel" + "_csv");

        strRes = results.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                String s = "Q0";
                int rank = 0;
                if(flagFrom)
                    return groupNum + " " + s + " " + row.getLong(2) + " " + rank + " " + new BigDecimal(row.getDouble(1)).toPlainString() + " " + filename;
                else if(flagLearningRes)
                    return groupNum + " " + s + " " + row.getLong(0) + " " + rank + " " + new BigDecimal(row.getDouble(1)).toPlainString() + " " + filename;
                else
                    return groupNum + " " + s + " " + row.getLong(0) + " " + rank + " " + new BigDecimal(row.getDouble(1)).toPlainString() + " " + filename;
            }
        });
        strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qtop1" + "_csv");
        tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qtop1" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qtop1" + ".csv");
        tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename + "_qtop1" + "_csv");
        tweetUtil.runStringCommand("sort -rn -k5,5 " + outPath + "out_" + filename + "_qtop1" + ".csv > " + outPath + "out_" + filename + "_qtop2" + ".csv");
        tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename + "_qtop1" + ".csv");
        FileReader fileReaderA = new FileReader(outPath + "out_" + filename + "_qtop2" + ".csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outPath + "out_" + filename + "_qtop" + ".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        String [] strs; int ind = 0;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(" ");
            bw.write(strs[0] + " " + strs[1] + " " + strs[2] + " " + ind + " " + strs[4] + " " + strs[5] + "\n");
        }
        bw.close();
        bufferedReaderA.close();
        tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qtop2" + ".csv");
        return 0;
    }

    public static int readTrecResults(String outPath, String filename, BufferedWriter bw, String topic, String folderName) throws IOException, InterruptedException {
        /**/
        filename = filename.split("_")[2];
        FileReader fileReaderA = new FileReader(outPath + "out_" + filename + ".csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        int ind = 0;
        bw.write(topic + "," + folderName + "," + filename + ",");
        while((line = bufferedReaderA.readLine()) != null){
            ind++;
            if(ind == 2)
                bw.write(line.split("num_ret        \tall\t")[1] + ",");
            if(ind == 3)
                bw.write(line.split("num_rel        \tall\t")[1] + ",");
            if(ind == 5)
                bw.write(line.split("map            \tall\t")[1] + ",");
            if(ind == 20)
                bw.write(line.split("map_at_R       \tall\t")[1] + ",");
            if(ind == 60)
                bw.write(line.split("P100           \tall\t")[1] + ",");
            if(ind == 63)
                bw.write(line.split("P1000          \tall\t")[1] + ",");
        }
        bw.write("\n");
        bw.flush();
        bufferedReaderA.close();
        return 0;
    }

    public static int readLocationResults(DataFrame results, SQLContext sqlContext, int index, String filename) throws IOException, InterruptedException {
        /**/
        final String emo_regex2 = "\\([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee]\\)";//"\\p{InEmoticons}";
        FileReader fileReaderA = new FileReader(outputCSVPath +"out_"+filename+"_csv/part-00000");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        FileWriter fw = new FileWriter(outputCSVPath +"location_freq1.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        //FileWriter fwUserLoc = new FileWriter(outputCSVPath +"user_location_clean.csv");
        //BufferedWriter bwUserLoc = new BufferedWriter(fwUserLoc);
        String line;
        int numberOfLines = 0;
        String username, loc;
        String [] splits;
        Map<String, Set<String>> usernameLocMap = new HashMap<>();
        Map<String, Double> locMap = new HashMap<>();
        ValueComparator bvc = new ValueComparator(locMap);
        TreeMap<String, Double> sorted_map = new TreeMap(bvc);
        while((line = bufferedReaderA.readLine()) != null){
            Matcher matcher = Pattern.compile(emo_regex2).matcher(line);
            line = matcher.replaceAll("").trim();
            splits = line.split(",");
            if(splits.length < 2)
                continue;
            username = splits[0];
            for(int i = 1; i < splits.length; i++) {
                loc = splits[i].toLowerCase().replace(" ", "");
                if (locMap.containsKey(loc)) {
                    locMap.put(loc, locMap.get(loc) + 1);
                } else
                    locMap.put(loc, 1.0);
                numberOfLines++;
            }
        }
        sorted_map.putAll(locMap);
        for(Map.Entry<String, Double> entry : sorted_map.entrySet()) {
            bw.write(entry.getKey() + "," + entry.getValue() + "\n");
        }
        bw.close();
        return 0;
    }


    public static int writeLocationResults(String filename) throws IOException, InterruptedException {
        /**/
        final String emo_regex2 = "\\([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee]\\)";//"\\p{InEmoticons}";
        FileReader fileReaderA = new FileReader(outputCSVPath +"location_frequency.csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        FileWriter fwUserLoc = new FileWriter(outputCSVPath +"user_location_clean.csv");
        BufferedWriter bwUserLoc = new BufferedWriter(fwUserLoc);
        String line;
        int numberOfLines = 0;
        String username, loc;
        String [] splits;
        Map<String, Set<String>> usernameLocMap = new HashMap<>();
        Map<String, Double> locMap = new HashMap<>();
        while((line = bufferedReaderA.readLine()) != null) {
            splits = line.split(",");
            if (Double.valueOf(splits[1]) > configRead.getUserLocThreshold())
                locMap.put(splits[0], Double.valueOf(splits[1]));
        }
        fileReaderA.close();
        fileReaderA = new FileReader(outputCSVPath +"out_"+filename+"_csv/part-00000");
        bufferedReaderA = new BufferedReader(fileReaderA);
        while((line = bufferedReaderA.readLine()) != null) {
            Matcher matcher = Pattern.compile(emo_regex2).matcher(line);
            line = matcher.replaceAll("").trim();
            splits = line.split(",");
            if(splits.length < 2)
                continue;
            username = splits[0];
            for(int i = 1; i < splits.length; i++) {
                loc = splits[i].toLowerCase().replace(" ", "");
                if (locMap.containsKey(loc)) {
                    if (!usernameLocMap.containsKey(loc)) {
                        Set<String> users = new HashSet<>();
                        users.add(username);
                        usernameLocMap.put(loc, users);
                    } else {
                        Set<String> users = usernameLocMap.get(loc);
                        users.add(username);
                        usernameLocMap.put(loc, users);
                    }
                }
            }
        }
        for(String key : usernameLocMap.keySet()) {
            if(key.equals(""))
                continue;
            for(String user : usernameLocMap.get(key))
                bwUserLoc.write(user + "," + key + "\n");
        }
        fileReaderA.close();
        bwUserLoc.close();
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

    public static void makeScatterFiles() throws IOException {

        String[] hashtagCounts = {"CSVOut_hashtag_tweetCount_parquet.csv", "CSVOut_hashtag_userCount_parquet.csv"};
        String[] userCounts = {"CSVOut_mention_tweetCount_parquet.csv", "CSVOut_user_hashtagCount_parquet.csv", "CSVOut_user_tweetCount_parquet.csv"};
        String[] userFeatureCounts = {"CSVOut_user_favoriteCount_parquet.csv", "CSVOut_user_friendsCount_parquet.csv", "CSVOut_user_followerCount_parquet.csv", "CSVOut_user_statusesCount_parquet.csv"};
        String[] termCounts = {"CSVOut_term_tweetCount_parquet.csv"};
        String hashtagProbPath, hashtagUniqueCountPath, outputPath, outputPath2, commonPath, countName;
        HashMap<String, String[]> hashMap;
        List<ScatterPlot> objects;
        boolean flagCE, flag3, flag2;
        for(String topic : topics) {
            for (String subAlg : subAlgs) {
                for (String feature : features) {
                    for(int c = 0; c < 4; c++) {
                        if (feature.equals("Term") && c == 1)//no second count for term
                            continue;
                        commonPath =  clusterResultsPath + topic + "/" + subAlg + "/";
                        //if(subAlg.equals("CE")) // for CE, we consider only non-zero values
                        //    hashtagProbPath = commonPath + feature + "/NoZero_" + feature + "1.csv";
                        //else
                        if(topic.equals("Politics") && c ==0 && subAlg.equals("MI") && feature.equals("From"))
                            continue;
                        hashtagProbPath = commonPath + feature + "/" + feature + "1.csv";
                        switch (feature) {
                            case "Hashtag":
                                countName = hashtagCounts[c];
                                break;
                            case "Term":
                                countName =  termCounts[c];
                                break;
                            case "UserFeatures":
                                countName = userFeatureCounts[c];
                                break;
                            default:
                                countName =  userCounts[c];
                                break;
                        }
                        //hashtagUniqueCountPath = "/Volumes/SocSensor/Zahra/Sept16/ClusterResults/counts/name_numbers/" + countName;
                        hashtagUniqueCountPath = "/Volumes/SocSensor/Zahra/SocialSensor/userFeaturesCounts/" + countName;
                        outputPath = commonPath + feature + "_" + countName.split("[._]")[2] + "_" + subAlg + ".csv";
                        outputPath2 = commonPath + feature + "_" + countName.split("[._]")[2] + "_" + subAlg + ".csv";
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
                        System.out.println( hashtagProbPath + " --- " + hashtagUniqueCountPath);
                        System.out.println("outputPath: " + outputPath2);
                        while ((line = bufferedReaderB.readLine()) != null) {
                            strs = new String[2];
                            strs[0] = line.split(",")[1];
                            strs[1] = line.split(",")[0].toLowerCase();
                            if(strs.length <2)
                                continue;
                            if(Double.valueOf(strs[0]) < 5)
                                break;
                            if (!hashMap.containsKey(strs[1])) {
                                value = new String[2];
                                value[0] = strs[0];// first string is the tweetCount
                                value[1] = "";// second String is the probability
                                hashMap.put(strs[1], value);
                            }else {
                                System.out.println("Something wrong " + line);
                            }
                        }
                        System.out.println("First file done");
                        flag2 = false; flag3 = false;
                        while ((line = bufferedReaderA.readLine()) != null) { // Mention, 0.01, username
                            strs = line.split(",");
                            if(strs.length <2)
                                continue;
                            if(strs.length == 2 && !flag3){
                                flag2 = true;
                                if (hashMap.containsKey(strs[1])) {
                                    value = hashMap.get(strs[1]);
                                    value[1] = new BigDecimal(strs[0]).toPlainString();
                                    if(strs[0].equals("9.115169284401823E-11"))
                                        System.out.println(value[0] + " "+value[1]);
                                    hashMap.put(strs[1], value);
                                }
                            }else if(strs.length == 3){
                                flag3 = true;
                                if (hashMap.containsKey(strs[2])) {
                                    value = hashMap.get(strs[2]);
                                    value[1] = new BigDecimal(strs[1]).toPlainString();
                                    if(strs[1].equals("9.115169284401823E-11"))
                                        System.out.println(value[0] + " " +value[1]);
                                    hashMap.put(strs[2], value);
                                }
                            }else {
                                System.out.println("Something wrong " + line);
                            }/*else {
                                if(!subAlg.equals("CE"))
                                    System.out.println("Something wrong " + strs[0] + " " + strs[1] + "  " + strs[2]);
                            }*/
                        }
                        System.out.println("Second file done");
                        objects = new ArrayList<ScatterPlot>();
                        flagCE = subAlg.equals("CE");
                        for (String key : hashMap.keySet()) {
                            if(hashMap.get(key)[1].equals(""))
                                continue;
                            //objects.add(new ScatterPlot(Double.valueOf(hashMap.get(key)[1]), Double.valueOf(hashMap.get(key)[0]), key, flagCE));
                            objects.add(new ScatterPlot(Double.valueOf(hashMap.get(key)[1]), Double.valueOf(hashMap.get(key)[0]), key, flagCE));
                        }
                        Collections.sort(objects);
                        for (int i = 0; i < objects.size(); i++) {
                            if(i < 10)
                                bw2.write(objects.get(i).getSecondDimCount() + ","+objects.get(i).getFeatureValue()+","+objects.get(i).getFeatureKey()+","+1);
                            else if(i >= (objects.size()/2 - 5) && i < (objects.size()/2 + 5))
                                bw2.write(objects.get(i).getSecondDimCount() + ","+objects.get(i).getFeatureValue()+","+objects.get(i).getFeatureKey()+","+2);
                            else if(i >= objects.size()-10)
                                bw2.write(objects.get(i).getSecondDimCount() + ","+objects.get(i).getFeatureValue()+","+objects.get(i).getFeatureKey()+","+3);
                            else
                                bw2.write(objects.get(i).getSecondDimCount() + ","+objects.get(i).getFeatureValue()+","+""+",0");
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














