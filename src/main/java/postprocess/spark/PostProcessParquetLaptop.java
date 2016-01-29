package postprocess.spark;

import visualization1.ScatterPlot;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import preprocess.spark.ConfigRead;
import scala.Tuple2;
import util.TweetUtil;
import util.ValueComparator;

import java.io.*;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PostProcessParquetLaptop implements Serializable {
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
    private static boolean testFlag ;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String args[]) throws IOException, InterruptedException {

        loadConfig();
        testFlag = configRead.getTestFlag();
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
        boolean readLearningResults = false;
        boolean readNonzeroLearningWeights = false;
        boolean readTables = true;
        boolean readNonzeroBaselineMixedWeights = false;
        boolean readNonzeroBaselineMixedWeights2 = false;
        boolean readNBResults = false;
        boolean findTestTrainDataTids = false;
        boolean analyzeMI = false;
        boolean writeTableTopFeatureTopics = false;

        if(analyzeMI)
            analyzeMI();

        if(writeTableTopFeatureTopics)
            writeTableTopFeatureTopics();

        if(readTables || findTestTrainDataTids) {
            SparkConf sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
            SQLContext sqlContext = new SQLContext(sparkContext);
            String path = "ClusterResults/tidTextList_parquet";
            sqlContext.read().parquet(path).coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save("ClusterResults/tidTextList_csv");
            if(readTables)
                readResultsCSV2(sqlContext);
            else if(findTestTrainDataTids)
                findTestTrainDataFeatures(sqlContext);//findTestTrainDataTids();
        }

        if(readNBResults)
            readNBResults("", "", "", configRead.getGroupNames()[groupNum -1], -1, "NB");

        if(readNonzeroBaselineMixedWeights)
            readNonzeroBaselineMixedWeights();
        if(readNonzeroBaselineMixedWeights2)
            readNonzeroBaselineMixedWeights2();
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

        if(readNonzeroLearningWeights){
            readNonzeroLearningWeights();
        }

        if(convertParquet) {
            boolean readTestTrain = true;
            String path = "ClusterResults/tidTextList_parquet";
            String topic = configRead.getGroupNames()[groupNum-1];
            SparkConf sparkConfig;
            if (local) {
                tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV");
                tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV/" + topic);

                outputCSVPath = path;
                FileReader fileReaderA;
                BufferedReader bufferedReaderA;

                if(testFlag){
                    fileReaderA = new FileReader("TestSet/Data/Data/Learning/Topics/"+topic+"/featureData/featureIndex.csv");
                    outputCSVPath = "TestSet/Out/TestTrainData/"+topic+"/";
                }else
                    fileReaderA = new FileReader("Data/Learning/Topics/featureData/featureIndex.csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                hashtagMap = new HashMap<>();
                String line;
                int featCount = 0;
                String[] feats;
                int featureInd = 1, numberInd = 2;
                while ((line = bufferedReaderA.readLine()) != null) {
                    featCount++;
                    feats = line.split(",");
                    switch (feats[0]) {
                        case "from":
                            hashtagMap.put("from:" + feats[featureInd], Long.valueOf(feats[numberInd]));
                            break;
                        case "term":
                            hashtagMap.put("term:" + feats[featureInd], Long.valueOf(feats[numberInd]));
                            break;
                        case "hashtag":
                            hashtagMap.put("hashtag:" + feats[featureInd], Long.valueOf(feats[numberInd]));
                            break;
                        case "mention":
                            hashtagMap.put("mention:" + feats[featureInd], Long.valueOf(feats[numberInd]));
                            break;
                        case "location":
                            hashtagMap.put("location:" + feats[featureInd], Long.valueOf(feats[numberInd]));
                            break;
                    }
                    //hashtagMap.put(line.split(",")[0], Long.valueOf(line.split(",")[1]));
                }
                sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
            } else
                sparkConfig = new SparkConf().setAppName("PostProcessParquet");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
            SQLContext sqlContext = new SQLContext(sparkContext);

            // Read all parquet part by part results files and combine them into 1 csv file for each iteration per group
            File folder1 = new File(outputCSVPath);
            ArrayList<String> fileNames1 = listFilesForFolder(folder1);
            //for (String filename1 : fileNames1) {
                //if(!filename1.equals("TestTrainData")) continue;
//                if(testFlag)
//                    tweetUtil.runStringCommand("mkdir " + "TestSet/Out/TestTrainData/" + topic +"/"+filename1);
//                else
//                    tweetUtil.runStringCommand("mkdir " + "ClusterResults/Nov30/BaselinesResCSV/" +filename1);
//                File folder = new File(outputCSVPath+"/"+filename1);
//                ArrayList<String> fileNames = listFilesForFolder(folder);
                DataFrame res;
                int ind = -1;
                int[] lineNumbers = new int[fileNames1.size()];
                for (String filename : fileNames1) {
                    ind++;
                    if (filename.contains(".csv") || filename.contains("_csv") || filename.equals("out") || filename.contains("trecout_all_"))
                        continue;
                    System.out.println(outputCSVPath + "/" + filename);
                    res = sqlContext.read().parquet(outputCSVPath + "/" + filename);
                    if (readTestTrain) {
                        if (filename.contains("strings"))// && !filename.contains("_4_")&& !filename.contains("_5_")&& !filename.contains("_6_"))
                            lineNumbers[ind] = readResults2Strings(res, sparkContext, ind, filename);
                        else {
                            lineNumbers[ind] = readResults2(res, sparkContext, ind, filename, outputCSVPath + "TestTrainDataCSV/");
                            /*lineNumbers[ind] = readResults2Index(res, sparkContext, ind, filename, outputCSVPath + "TestTrainDataCSV/");
                            String outputCSVPath2 = "";
                            outputCSVPath2 = outputCSVPath + "/TestTrainDataCSV/";
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
                            bufferedReaderB.close();*/
                        }
                    }
                }
            //}
        }

        if(runScript) {
            // Combine all CSV files into one file for each group
            printForumla(itNum, hashtagNum);
            tweetUtil.runScript("cp " + scriptPath + "mergeFiles.sh " + outputCSVPath + "mergeFiles.sh");
            tweetUtil.runScript("chmod +x " + outputCSVPath + "mergeFiles.sh");
            tweetUtil.runScript("./" + outputCSVPath + "mergeFiles.sh");
        }
    }

    private static void readNonzeroLearningWeights() throws IOException, InterruptedException {
        String path = "TestSet/Data/LearningMethods/NB/Topics/";
//        if(!testFlag)
//            path = "Data/Learning/Topics/";
        String logisticMethod = "l2_lr/";
        logisticMethod = "";
        FileReader fileReaderA = new FileReader(path+ configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"featureWeights.csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(path + configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"nonZero_featureWeights.csv");
        BufferedWriter bw = new BufferedWriter(fw);
        FileWriter fw1 = new FileWriter(path + configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"featureWeights_from.csv");
        BufferedWriter bwFrom = new BufferedWriter(fw1);
        FileWriter fw2 = new FileWriter(path + configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"featureWeights_hashtag.csv");
        BufferedWriter bwHashtag = new BufferedWriter(fw2);
        FileWriter fw3 = new FileWriter(path + configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"featureWeights_location.csv");
        BufferedWriter bwLocation = new BufferedWriter(fw3);
        FileWriter fw4 = new FileWriter(path + configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"featureWeights_mention.csv");
        BufferedWriter bwMention = new BufferedWriter(fw4);
        FileWriter fw5 = new FileWriter(path + configRead.getGroupNames()[groupNum-1] +"/fold1000/"+logisticMethod+"featureWeights_term.csv");
        BufferedWriter bwTerm = new BufferedWriter(fw5);
        String stF, stW, feat, featType;
        while((line = bufferedReaderA.readLine()) != null){
            stF = line.split(",")[0];
            stW = line.split(",")[1];
            featType = stF.split(":")[0];
            feat = stF.split(":")[1];
            if(!stW.equals("0")) {
                bw.write(featType+","+feat+","+stW+"\n");
                if(featType.equals("from"))
                    bwFrom.write(featType+","+feat+","+stW+"\n");
                if(featType.equals("hashtag"))
                    bwHashtag.write(featType+","+feat+","+stW+"\n");
                if(featType.equals("location"))
                    bwLocation.write(featType+","+feat+","+stW+"\n");
                if(featType.equals("mention"))
                    bwMention.write(featType+","+feat+","+stW+"\n");
                if (featType.equals("term"))
                    bwTerm.write(featType+","+feat+","+stW+"\n");
            }
        }
        bw.close();
        bwFrom.close();bwMention.close();bwHashtag.close();bwLocation.close();bwTerm.close();
        fileReaderA.close();
        tweetUtil.runStringCommand("chmod +x " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights.csv");
        tweetUtil.runStringCommand("sort -t',' -rn -k3,3 " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights.csv > " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights1.csv");
        tweetUtil.runStringCommand("rm -f " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights.csv");
        tweetUtil.runStringCommand("mv " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights1.csv > " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod+"/nonZero_featureWeights.csv");
    }

    private static void readBaselineResults(boolean local) throws IOException, InterruptedException {
        boolean readTrecResults = true;
        String outputPath = "";
        String topic = configRead.getGroupNames()[groupNum-1];
        SparkConf sparkConfig;
        if (local) {
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV");
            tweetUtil.runStringCommand("mkdir " + "ClusterResults/BaselinesResCSV/" + topic);
            outputCSVPath = "ClusterResults/Nov30/BaselinesRes/Baselines/Topics/"+topic+"/";
            outputPath = "ClusterResults/Nov30/BaselinesRes/Out/Baselines/Topics/";
            if(readTrecResults) {
                FileWriter fwTrec = new FileWriter(outputPath + "/" + "trecout_all_" + topic + ".csv");
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
                if(filename.contains("_realCSV")) continue;
                //if (filename.contains(".csv") || filename.contains("_csv") || filename.equals("out")) continue;
                System.out.println(outputCSVPath + "/" + filename1+"/" + filename);
                if(readTrecResults){
                    readTrecResults(outputPath + topic + "/" + filename1 + "/", filename, bwTrec, topic, filename1);
                }else {
                    if (!filename.contains("_noTrainTweet")) {
                        res = sqlContext.read().parquet(outputCSVPath + "/" + filename1 + "/" + filename);
                        readBaselineResultFiles(res, null, filename, outputPath + topic + "/" + filename1 + "/");
                    } else if (filename.contains("_noTrainTweet")) {
                        res = sqlContext.read().parquet(outputCSVPath + "/" + filename1 + "/" + filename);
                        readBaselineResultFiles(null, res, filename, outputPath + topic + "/" + filename1 + "/");
                    }
                }

/*

                    res = sqlContext.read().parquet(outputCSVPath + "/" + filename1 + "/" + filename);
                    System.out.println("LOOK: ");
                    res.printSchema();
                    lineNumbers[ind] = readResults1(res, sqlContext, ind, filename, "ClusterResults/BaselinesResCSV/" + topic + "/" + filename1 + "/", false);
                }*/
            }
        }
        if(readTrecResults)
            bwTrec.close();
    }
    private static void readLearningResults(boolean local) throws IOException, InterruptedException {
        String clusterPath = "ClusterResults/Dec16/BaselinesRes/LearningMethods/NB/Topics/";
        String clusterOutPath = "ClusterResults/Dec16/BaselinesRes/LearningMethods/NB/Out/Topics/";
        if(testFlag) {
            clusterPath = "TestSet/Data/BaselinesRes/LearningMethods/NB/Topics/";
            clusterOutPath = "TestSet/Data/BaselinesRes/LearningMethods/NB/Topics/";
        }
        boolean readTrecResults = false;
        String topic = configRead.getGroupNames()[groupNum-1];
        SparkConf sparkConfig;
        if (local) {
            tweetUtil.runStringCommand("mkdir " + clusterOutPath);
            tweetUtil.runStringCommand("mkdir " + clusterOutPath + topic);
            outputCSVPath = clusterPath+topic+"/";
            if(readTrecResults) {
                FileWriter fwTrec = new FileWriter(clusterOutPath+ topic + "/" + "trecout_all_" + topic + ".csv");
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
            //tweetUtil.runStringCommand("mkdir " + clusterOutPath + topic +"/"+filename1);
            DataFrame res;
            int ind = -1;
            int[] lineNumbers = new int[fileNames1.size()];
            ind++;
            System.out.println(outputCSVPath + "/" + filename1+"/" + filename1);
            if(readTrecResults){
                readTrecResults(clusterPath+topic+"/",filename1, bwTrec, topic, filename1);
            }else {
                //if(!filename1.contains("_all")) continue;
                res = sqlContext.read().parquet(outputCSVPath + "/" + filename1);
                if(filename1.contains("_noTrainTweet_")) {
                    readBaselineResultFiles(null, res, filename1, clusterOutPath + topic + "/");
                    //readLearningResultFiles(sqlContext, null, res, filename1, clusterOutPath + topic + "/", true);
                }else{
                    readBaselineResultFiles(res, null, filename1, clusterOutPath + topic + "/");
                    //readLearningResultFiles(sqlContext, res, null, filename1, clusterOutPath + topic + "/", true);
                }
                /*res = sqlContext.read().parquet(outputCSVPath + "/" + filename1);
                System.out.println("LOOK: ");
                res.printSchema();
                lineNumbers[ind] = readResults1(res, sqlContext, ind, filename1, clusterOutPath+ topic + "/", true);*/
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

    public static int readResults2(DataFrame results, JavaSparkContext sc, int index, String filename, String outputCSVPath2) throws IOException, InterruptedException {
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
        //String outputCSVPath2 = "ClusterResults/TestTrainDataCSV/";
        final String emo_regex2 = "\\([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee]\\)";//"\\p{InEmoticons}";

        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD strRes = results.select("username", "term", "hashtag", "mentionee", "location", "time", "tid").javaRDD().map(new Function<Row, String>() {
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
                    String loc = row.getString(4);
                    Matcher matcher = Pattern.compile(emo_regex2).matcher(loc);
                    loc = matcher.replaceAll("").trim();
                    loc = loc.toLowerCase().replace(" ", "");
                    out += "location:"+loc+ " ";
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
                    return "-1 " + out + time + " " + tid;
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

    public static int readResults2Index(DataFrame results, JavaSparkContext sc, int index, String filename, String outputCSVPath2) throws IOException, InterruptedException {
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
        //String outputCSVPath2 = "ClusterResults/TestTrainDataCSV/";

        final Accumulator<Integer> accumulator = sc.accumulator(0);
        JavaRDD strRes = results.select("username", "term", "hashtag", "mentionee", "location", "time", "tid", "topical").javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String topical = "-1"; String[] features;
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
        filename = "/Volumes/SocSensor/Zahra/FeatureStatisticsRun_Sept1/ClusterResults/Disaster/MI/From/CSVOut_mutualEntropyTweetFromUse._1()_parquet.csv";
        FileReader fileReaderA = new FileReader(filename);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(filename + "_1.csv");
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

    public static void readLearningResultFiles(SQLContext sqlContext, DataFrame resultsTestTrain, final DataFrame resultsNoTrain, final String filename, String outPath, boolean flagLearningRes) throws IOException, InterruptedException {
        JavaRDD<String> strRes;
        final List<String> trainHashtags = tweetUtil.getTestTrainGroupHashtagList(groupNum, testFlag, true);
        final List<String> testHashtags = tweetUtil.getTestTrainGroupHashtagList(groupNum, testFlag, false);
        if (flagLearningRes) {
            DataFrame results;
            if (resultsNoTrain != null)
                results = resultsNoTrain;
            else
                results = resultsTestTrain;
            StructField[] fieldsMap = {
                    DataTypes.createStructField("tid", DataTypes.LongType, true),
                    DataTypes.createStructField("text", DataTypes.StringType, true),
                    DataTypes.createStructField("prob", DataTypes.DoubleType, true)
            };
            results = sqlContext.createDataFrame(results.javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Row row) throws Exception {
                    String out = "";
                    int ind = 1;
                    if (row.get(ind) != null) out += " - prob: " + row.get(ind).toString();
                    ind++;
                    int topical = 0;
                    if (row.get(5) != null) {
                        List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(5).split(" ")));
                        if (resultsNoTrain != null) {
                            List<String> hashtags2 = new ArrayList<String>();
                            hashtags2.addAll(hashtags);
                            hashtags.retainAll(trainHashtags);
                            hashtags2.retainAll(testHashtags);
                            topical = (hashtags2.size() > 0) ? 1 : 0;
                            if (hashtags.size() > 0 || hashtags2.size() > 0)
                                return new Tuple2<Long, String>(row.getLong(0), "-1");
                        } else {
                            hashtags.retainAll(testHashtags);
                            topical = (hashtags.size() > 0) ? 1 : 0;
                        }
                    }
                    if (row.get(ind) != null) out += " - topical: " + topical;
                    ind++;
                    if (row.get(ind) != null) out += " - username: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - term: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - hashtag: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - mention: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - location: " + row.getString(ind);
                    ind++;
                    return new Tuple2<Long, String>(row.getLong(0), out);
                }
            }).filter(new Function<Tuple2<Long, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Long, String> v1) throws Exception {
                    return !v1._2().equals("-1");
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String v1, String v2) throws Exception {
                    Set<String> settmp = new HashSet<String>();
                    settmp.addAll(Arrays.asList(v1.split(" - ")));
                    settmp.addAll(Arrays.asList(v2.split(" - ")));
                    String out = "";
                    for (Object s : settmp.toArray())
                        out += " - " + s.toString();
                    return out;
                }
            }).map(new Function<Tuple2<Long, String>, Row>() {
                @Override
                public Row call(Tuple2<Long, String> v1) throws Exception {
                    return RowFactory.create(v1._1(), v1._2(), Double.valueOf(v1._2().split(" - prob: ")[1].split(" -")[0]));
                }
            }), new StructType(fieldsMap));
            results.sort(results.col("prob").desc()).javaRDD().coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_csv/part-00000" + " " + outPath + "out_" + filename + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_csv");
        }


        if (resultsTestTrain != null) {
            StructField[] fieldsIDHashtag = {
                    DataTypes.createStructField("tid", DataTypes.LongType, true),
                    DataTypes.createStructField("hashtag", DataTypes.StringType, true)
            };
            DataFrame results1 = sqlContext.createDataFrame(resultsTestTrain.select("tid", "hashtag").javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Row row) throws Exception {
                    return new Tuple2<Long, String>(row.getLong(0), row.getString(1));
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String v1, String v2) throws Exception {
                    Set<String> settmp = new HashSet<String>();
                    if (v1 != null)
                        settmp.addAll(Arrays.asList(v1.split(" ")));
                    if (v2 != null)
                        settmp.addAll(Arrays.asList(v2.split(" ")));
                    String out = "";
                    for (Object s : settmp.toArray())
                        out += " " + s.toString();
                    return out;
                }
            }).map(new Function<Tuple2<Long, String>, Row>() {
                @Override
                public Row call(Tuple2<Long, String> v1) throws Exception {
                    return RowFactory.create(v1._1(), v1._2());
                }
            }), new StructType(fieldsIDHashtag));

            DataFrame df = resultsTestTrain.select("tid", "topical", "prob").distinct();
            df = df.join(results1, df.col("tid").equalTo(results1.col("tid"))).drop(results1.col("tid"));
            strRes = df.distinct().sort(df.col("prob").desc()).javaRDD().map(new Function<Row, String>() {
                //strRes = resultsTestTrain.select("tid", "topical", "prob").distinct().sort(resultsTestTrain.col("prob").desc()).javaRDD().map(new Function<Row, String>() {//.distinct()
                @Override
                public String call(Row row) throws Exception {
                    String s = "Q0";
                    List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(3).split(" ")));
                    List<String> hashtags2 = new ArrayList<String>();
                    hashtags2.addAll(hashtags);
                    hashtags.retainAll(trainHashtags);
                    hashtags2.retainAll(testHashtags);
                    int topical = (hashtags2.size() > 0 || hashtags.size() > 0) ? 1 : 0;
                    return groupNum + " " + s + " " + row.getLong(0) + " " + topical;
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qrel" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qrel" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qrel" + "_csv");


            strRes = resultsTestTrain.select("prob", "tid").distinct().sort(resultsTestTrain.col("prob").desc()).javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {
                @Override
                public String call(Tuple2<Row, Long> v1) throws Exception {
                    String s = "Q0";
                    if (v1._1().get(0).toString().contains("NaN") || v1._1().get(0).toString().contains("Inf"))
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + v1._1().get(0).toString() + " " + filename;
                    else
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + new BigDecimal(v1._1().getDouble(0)).toPlainString() + " " + filename;
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qtop" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qtop" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qtop" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qtop" + "_csv");
        }

        if (resultsNoTrain != null) {
            StructField[] fieldsIDHashtag = {
                    DataTypes.createStructField("tid", DataTypes.LongType, true),
                    DataTypes.createStructField("hashtag", DataTypes.StringType, true)
            };
            DataFrame results1 = sqlContext.createDataFrame(resultsNoTrain.select("tid", "hashtag").javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Row row) throws Exception {
                    return new Tuple2<Long, String>(row.getLong(0), row.getString(1));
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String v1, String v2) throws Exception {
                    Set<String> settmp = new HashSet<String>();
                    if (v1 != null)
                        settmp.addAll(Arrays.asList(v1.split(" ")));
                    if (v2 != null)
                        settmp.addAll(Arrays.asList(v2.split(" ")));
                    String out = "";
                    for (Object s : settmp.toArray())
                        out += " " + s.toString();
                    return out;
                }
            }).map(new Function<Tuple2<Long, String>, Row>() {
                @Override
                public Row call(Tuple2<Long, String> v1) throws Exception {
                    return RowFactory.create(v1._1(), v1._2());
                }
            }), new StructType(fieldsIDHashtag));

            DataFrame df = resultsNoTrain.select("tid", "topical", "prob").distinct();
            df = df.join(results1, df.col("tid").equalTo(results1.col("tid"))).drop(results1.col("tid"));
            strRes = df.distinct().sort(df.col("prob").desc()).javaRDD().map(new Function<Row, String>() {
                //strRes = resultsNoTrain.select("tid", "topical", "prob", "hashtag").distinct().sort(resultsNoTrain.col("prob").desc()).javaRDD().map(new Function<Row, String>() {//.distinct()
                @Override
                public String call(Row row) throws Exception {
                    String s = "Q0";
                    int topical;
                    if (row.getString(3) == null)
                        topical = 0;
                    else {
                        List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(3).split(" ")));
                        hashtags.retainAll(testHashtags);
                        topical = (hashtags.size() > 0) ? 1 : 0;
                    }
                    return groupNum + " " + s + " " + row.getLong(0) + " " + topical;
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv");


            strRes = resultsNoTrain.select("prob", "tid").distinct().sort(resultsNoTrain.col("prob").desc()).javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {
                @Override
                public String call(Tuple2<Row, Long> v1) throws Exception {
                    String s = "Q0";
                    if (v1._1().get(0).toString().contains("NaN") || v1._1().get(0).toString().contains("Inf"))
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + v1._1().get(0).toString() + " " + filename;
                    else
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + new BigDecimal(v1._1().getDouble(0)).toPlainString() + " " + filename;
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + "_csv");
        }
    }
    public static void readBaselineResultFiles(DataFrame resultsTestTrain, DataFrame resultsNoTrain, final String filename, String outPath) throws IOException, InterruptedException {
        JavaRDD<String> strRes;
        final boolean NB = true;
        if(resultsTestTrain != null) {
            strRes = resultsTestTrain.select("tid", "topical", "prob").distinct().sort((NB) ? resultsTestTrain.col("prob").asc() : resultsTestTrain.col("prob").desc()).javaRDD().map(new Function<Row, String>() {//.distinct()
                @Override
                public String call(Row row) throws Exception {
                    String s = "Q0";
                    if (row.get(1) == null)
                        return groupNum + " " + s + " " + row.getLong(0) + " " + "0";
                    return groupNum + " " + s + " " + row.getLong(0) + " " + row.getInt(1);
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qrel" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qrel" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qrel" + "_csv");


            strRes = resultsTestTrain.select("prob", "tid").distinct().sort((NB) ? resultsTestTrain.col("prob").asc() : resultsTestTrain.col("prob").desc()).javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {
                @Override
                public String call(Tuple2<Row, Long> v1) throws Exception {
                    String s = "Q0";
                    if (v1._1().get(0).toString().contains("NaN") || v1._1().get(0).toString().contains("Inf"))
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + v1._1().get(0).toString() + " " + filename;
                    else
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + new BigDecimal((NB)? -v1._1().getDouble(0) : v1._1().getDouble(0)).toPlainString() + " " + filename;
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qtop" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qtop" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qtop" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qtop" + "_csv");
        }

        if(resultsNoTrain != null) {
            strRes = resultsNoTrain.select("tid", "topical", "prob").distinct().sort((NB) ? resultsTestTrain.col("prob").asc() : resultsNoTrain.col("prob").desc()).javaRDD().map(new Function<Row, String>() {//.distinct()
                @Override
                public String call(Row row) throws Exception {
                    String s = "Q0";
                    return groupNum + " " + s + " " + row.getLong(0) + " " + row.getInt(1);
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv");


            strRes = resultsNoTrain.select("prob", "tid").distinct().sort((NB) ? resultsTestTrain.col("prob").asc() : resultsNoTrain.col("prob").desc()).javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, String>() {
                @Override
                public String call(Tuple2<Row, Long> v1) throws Exception {
                    String s = "Q0";
                    if (v1._1().get(0).toString().contains("NaN") || v1._1().get(0).toString().contains("Inf"))
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + v1._1().get(0).toString() + " " + filename;
                    else
                        return groupNum + " " + s + " " + v1._1().getLong(1) + " " + v1._2() + " " + new BigDecimal((NB)? -v1._1().getDouble(0) : v1._1().getDouble(0)).toPlainString() + " " + filename;
                }
            });
            strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qtop" + "_csv");
        }
    }

    public static int readResults1(DataFrame results, SQLContext sqlContext, int index, final String filename, String outPath, boolean flagLearningRes) throws IOException, InterruptedException {
        /**/
        final boolean noTrainFlag = false;
        StructField[] fieldsIDHashtag = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        final List<String> trainHashtags = tweetUtil.getTestTrainGroupHashtagList(groupNum, false, true);
        final List<String> testHashtags = tweetUtil.getTestTrainGroupHashtagList(groupNum, false, false);
        DataFrame results1 = null;
        if(flagLearningRes) {
            StructField[] fieldsMap = {
                    DataTypes.createStructField("tid", DataTypes.LongType, true),
                    DataTypes.createStructField("text", DataTypes.StringType, true),
                    DataTypes.createStructField("prob", DataTypes.DoubleType, true)
            };
            results1 = sqlContext.createDataFrame(results.javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
                @Override
                public Tuple2<Long, String> call(Row row) throws Exception {
                    String out = "";
                    int ind = 1;
                    if (row.get(ind) != null) out += " - prob: " + row.get(ind).toString();
                    ind++;
                    int topical = 0;
                    if(row.get(5) != null) {
                        List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(5).split(" ")));
                        if(noTrainFlag) {
                            List<String> hashtags2 = new ArrayList<String>();
                            hashtags2.addAll(hashtags);
                            hashtags.retainAll(trainHashtags);
                            hashtags2.retainAll(testHashtags);
                            topical = (hashtags2.size() > 0) ? 1 : 0;
                            if (hashtags.size() > 0)
                                return new Tuple2<Long, String>(row.getLong(0), "-1");
                        }else{
                            hashtags.retainAll(testHashtags);
                            topical = (hashtags.size() > 0) ? 1 : 0;
                        }
                    }
                    if (row.get(ind) != null) out += " - topical: " + topical;
                    ind++;
                    if (row.get(ind) != null) out += " - username: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - term: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - hashtag: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - mention: " + row.getString(ind);
                    ind++;
                    if (row.get(ind) != null) out += " - location: " + row.getString(ind);
                    ind++;
                    return new Tuple2<Long, String>(row.getLong(0), out);
                }
            }).filter(new Function<Tuple2<Long, String>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Long, String> v1) throws Exception {
                    return !v1._2().equals("-1");
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String v1, String v2) throws Exception {
                    Set<String> settmp = new HashSet<String>();
                    settmp.addAll(Arrays.asList(v1.split(" - ")));
                    settmp.addAll(Arrays.asList(v2.split(" - ")));
                    String out = "";
                    for (Object s : settmp.toArray())
                        out += " - " + s.toString();
                    return out;
                }
            }).map(new Function<Tuple2<Long, String>, Row>() {
                @Override
                public Row call(Tuple2<Long, String> v1) throws Exception {
                    return RowFactory.create(v1._1(), v1._2(), Double.valueOf(v1._2().split(" - prob: ")[1].split(" -")[0]));
                }
            }), new StructType(fieldsMap));
            results1.sort(results1.col("prob").desc()).javaRDD().coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_csv");
            tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_csv/part-00000" + " " + outPath + "out_" + filename + ".csv");
            tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_csv");
        }
        System.out.println("DONE");

        results1 = sqlContext.createDataFrame(results.select("tid", "hashtag").javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), row.getString(1));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                Set<String> settmp = new HashSet<String>();
                if (v1 != null)
                    settmp.addAll(Arrays.asList(v1.split(" ")));
                if (v2 != null)
                    settmp.addAll(Arrays.asList(v2.split(" ")));
                String out = "";
                for (Object s : settmp.toArray())
                    out += " " + s.toString();
                return out;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> v1) throws Exception {
                return RowFactory.create(v1._1(), v1._2());
            }
        }), new StructType(fieldsIDHashtag));

        DataFrame df = results.select("tid", "topical", "prob").distinct();
        df = df.join(results1, df.col("tid").equalTo(results1.col("tid"))).drop(results1.col("tid"));
        JavaRDD strRes = df.sort(df.col("prob").desc()).javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                String s = "Q0";
                List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(3).split(" ")));
                if(noTrainFlag) {
                    List<String> hashtags2 = new ArrayList<String>();
                    hashtags2.addAll(hashtags);
                    hashtags.retainAll(trainHashtags);
                    hashtags2.retainAll(testHashtags);
                    int topical = (hashtags2.size() > 0) ? 1 : 0;
                    if (hashtags.size() > 0)
                        return "-1";
                    else
                        return groupNum + " " + s + " " + row.getLong(0) + " " + topical;
                }else {
                    hashtags.retainAll(testHashtags);
                    int topical = (hashtags.size() > 0) ? 1 : 0;
                    return groupNum + " " + s + " " + row.getLong(0) + " " + topical;
                }
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return !v1.equals("-1");
            }
        });
        strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qrel" + "_csv");
        tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qrel" + ".csv");
        tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qrel" + "_csv");


        strRes = df.distinct().sort(df.col("prob").desc()).javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                String s = "Q0";
                if(row.get(3) != null) {
                    List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(3).split(" ")));
                    List<String> hashtags2 = new ArrayList<String>();
                    hashtags2.addAll(hashtags);
                    hashtags.retainAll(trainHashtags);
                    hashtags2.retainAll(testHashtags);
                    if(noTrainFlag) {
                        int topical = (hashtags2.size() > 0) ? 1 : 0;
                        if (hashtags.size() > 0) //containsTrain
                            return "-1";//return groupNum + " " + s + " " + row.getLong(0) + " " + "0";
                        else
                            return groupNum + " " + s + " " + row.getLong(0) + " " + topical;
                    }else{
                        int topical = (hashtags.size() > 0 || hashtags2.size() > 0) ? 1 : 0;
                        return groupNum + " " + s + " " + row.getLong(0) + " " + topical;
                    }
                }else {
                    return groupNum + " " + s + " " + row.getLong(0) + " " + "0";
                }
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return !v1.equals("-1");
            }
        });
        strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv");
        tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + ".csv");
        tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename + "_withoutTestTrainOverlap_qrel" + "_csv");

        //.join(results1, results.col("tid").equalTo(results1.col("tid")), "right").drop(results.col("tid")).distinct()
        df = results.select("tid").distinct();
        df = results.select("prob", "tid").distinct().join(df, results.col("tid").equalTo(df.col("tid")), "right").drop(results.col("tid")).distinct().sort(results.col("prob").desc());
        df = df.join(results1, df.col("tid").equalTo(results1.col("tid"))).drop(results1.col("tid"));;
        strRes = df.javaRDD().map(new Function<Row, String>() {//.distinct()
            @Override
            public String call(Row row) throws Exception {
                String s = "Q0";
                int rank = 0;
                if(noTrainFlag) {
                    List<String> hashtags = new ArrayList<String>(Arrays.asList(row.getString(2).split(" ")));
                    List<String> hashtags2 = new ArrayList<String>();
                    hashtags2.addAll(hashtags);
                    hashtags.retainAll(trainHashtags);
                    hashtags2.retainAll(testHashtags);
                    if (hashtags.size() > 0) //containsTrain
                        return "-1";//return groupNum + " " + s + " " + row.getLong(0) + " " + "0";
                }
                if (row.get(0).toString().contains("NaN") || row.get(0).toString().contains("Inf"))
                    return groupNum + " " + s + " " + row.getLong(1) + " " + rank + " " + row.get(0).toString() + " " + filename;
                else
                    return groupNum + " " + s + " " + row.getLong(1) + " " + rank + " " + new BigDecimal(row.getDouble(0)).toPlainString() + " " + filename;

            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return !v1.equals("-1");
            }
        });
        strRes.coalesce(1).saveAsTextFile(outPath + "out_" + filename + "_qtop1" + "_csv");
        tweetUtil.runStringCommand("mv " + outPath + "out_" + filename + "_qtop1" + "_csv/part-00000" + " " + outPath + "out_" + filename + "_qtop1" + ".csv");
        tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename + "_qtop1" + "_csv");
        //tweetUtil.runStringCommand("sort -rn -k5,5 " + outPath + "out_" + filename + "_qtop1" + ".csv > " + outPath + "out_" + filename + "_qtop2" + ".csv");
        //tweetUtil.runStringCommand("rm -rf "+outPath + "out_" + filename + "_qtop1" + ".csv");
        FileReader fileReaderA = new FileReader(outPath + "out_" + filename + "_qtop1" + ".csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outPath + "out_" + filename + "_qtop" + ".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        String [] strs; int ind = 0;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(" ");
            bw.write(strs[0] + " " + strs[1] + " " + strs[2] + " " + ind + " " + strs[4] + " " + strs[5] + "\n");
            ind++;
        }
        bw.close();
        bufferedReaderA.close();
        tweetUtil.runStringCommand("rm -rf " + outPath + "out_" + filename + "_qtop1" + ".csv");
        return 0;
    }

    public static int readTrecResults(String outPath, String filename, BufferedWriter bw, String topic, String folderName) throws IOException, InterruptedException {
        /**/
        String fname = "";
        if(filename.contains("From"))
            fname = "From";
        else if(filename.contains("Hashtag"))
            fname = "Hashtag";
        else if(filename.contains("Mention"))
            fname = "Mention";
        else if(filename.contains("Location"))
            fname = "Location";
        else if(filename.contains("Term"))
            fname = "Term";

        if(filename.contains("noTrainTweet"))
            fname += "_noTrainTweet";
        else
            fname += "_TestTrain";
        FileReader fileReaderA = new FileReader(outPath + "out_" + fname + ".csv");
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
            if(ind == 61)
                bw.write(line.split("P200           \tall\t")[1] + ",");
            if(ind == 62)
                bw.write(line.split("P500           \tall\t")[1] + ",");
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


    public static int readResultsCSV2(SQLContext sqlContext) throws IOException, InterruptedException {
        //if(!filename.startsWith("From") || !filename.startsWith("Mention") || !filename.startsWith("Hashtag"))
        //    return 0;
        String parquetsPath = "/Volumes/SocSensor/nov7/tweet_thsh_mentionFeature_time_grouped_parquet/";
        String outputPath = "/Volumes/SocSensor/nov7-Out/tweet_thsh_mentionFeature_time_grouped_csv/";
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        FileWriter fw;
        BufferedWriter bw;
        String[] strs;
        double val;
        String line;
        DataFrame res;
        String fname = "";
        //for(String fname :listFilesForFolder(new File(parquetsPath))) {
        if(!fname.startsWith("_") && !(new File(outputPath+fname+"_csv").exists())) {
            sqlContext.read().parquet(parquetsPath + fname).write().format("com.databricks.spark.csv").save(outputPath + fname + "_csv");
        }
            /*fileReaderA = new FileReader(parquetsPath + fname);
            bufferedReaderA = new BufferedReader(fileReaderA);
            fw = new FileWriter(outputPath + fname);
            bw = new BufferedWriter(fw);
            while ((line = bufferedReaderA.readLine()) != null) {
                strs = line.split(",");
                line
                bw.write("\n");
            }
            bw.close();
            bufferedReaderA.close();*/
        //}
        return 0;
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
        int[] lineNum_5_10_50_100 = new int[4];
        for(int i = 0; i < 4; i++)
            lineNum_5_10_50_100[i] = 0;
        int numberOfLines = 0;
        String[] strs;
        while((line = bufferedReaderA.readLine()) != null){
            strs = line.split(",");
            strs[1] = new BigDecimal(strs[1]).toPlainString();
            numberOfLines++;
            if(numberOfLines < 571)
                continue;
            if(Double.valueOf(strs[1]) < 100) {
                lineNum_5_10_50_100[3]++;
                if(Double.valueOf(line.split(",")[1]) < 50)
                    lineNum_5_10_50_100[2]++;
                if(Double.valueOf(line.split(",")[1]) < 10)
                    lineNum_5_10_50_100[1]++;
                if(Double.valueOf(line.split(",")[1]) < 5)
                    lineNum_5_10_50_100[0]++;
                continue;
            }
            bw.write(strs[0] + "," + strs[1]);
            bw.write("\n");
        }
        bw.close();
        fileReaderA.close();
        System.out.println("Number of Terms with less than 5 occurences: " + lineNum_5_10_50_100[0]);
        System.out.println("Number of Terms with less than 10 occurences: " + lineNum_5_10_50_100[1]);
        System.out.println("Number of Terms with less than 50 occurences: " + lineNum_5_10_50_100[2]);
        System.out.println("Number of Terms with less than 100 occurences: " + lineNum_5_10_50_100[3]);
        System.out.println("Number of Terms retained after cleaning: " + (numberOfLines - lineNum_5_10_50_100[3]));
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

    private static void readNonzeroBaselineMixedWeights() throws IOException, InterruptedException {
        String path = "TestSet/Data/Data/Learning/Topics/";
        if(!testFlag)
            path = "ClusterResults/Nov27Res/Baselines/Out/";
        TweetUtil.runStringCommand("mkdir " + path);
        String outputPath = "ClusterResults/Nov27Res/";

        String logisticMethod = "l2_lr";
        String[] algNames = new String[]{ "MI"};//"topical", "topicalLog", "MILog", "CP", "CPLog",

        for (String algName : algNames) {
            path = "ClusterResults/Nov27Res/Baselines/Out/" + algName + "/";
            TweetUtil.runStringCommand("mkdir " + path);
            path += "Data/";
            TweetUtil.runStringCommand("mkdir " + path);
            path+= "Mixed5000/";
            TweetUtil.runStringCommand("mkdir " + path);
            path += "Topics/";
            TweetUtil.runStringCommand("mkdir " + path);
            TweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[groupNum - 1]);
            TweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/");
            TweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod);;
            FileReader fileReaderA = new FileReader(outputPath + "Baselines/" + configRead.getGroupNames()[groupNum - 1] + "/" + algName + "/Mixed5000/featureWeights.csv/part-00000");

            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
            String line;
            FileWriter fw = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights.csv");
            BufferedWriter bw = new BufferedWriter(fw);
            FileWriter fw1 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_from.csv");
            BufferedWriter bwFrom = new BufferedWriter(fw1);
            FileWriter fw2 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_hashtag.csv");
            BufferedWriter bwHashtag = new BufferedWriter(fw2);
            FileWriter fw3 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_location.csv");
            BufferedWriter bwLocation = new BufferedWriter(fw3);
            FileWriter fw4 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_mention.csv");
            BufferedWriter bwMention = new BufferedWriter(fw4);
            FileWriter fw5 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_term.csv");
            BufferedWriter bwTerm = new BufferedWriter(fw5);
            while ((line = bufferedReaderA.readLine()) != null) {
                if (!line.split(",")[2].equals("0")) {
                    bw.write(line + "\n");
                    if (line.split(",")[0].equals("From"))
                        bwFrom.write(line + "\n");
                    if (line.split(",")[0].equals("Hashtag"))
                        bwHashtag.write(line + "\n");
                    if (line.split(",")[0].equals("Location"))
                        bwLocation.write(line + "\n");
                    if (line.split(",")[0].equals("Mention"))
                        bwMention.write(line + "\n");
                    if (line.split(",")[0].equals("Term"))
                        bwTerm.write(line + "\n");
                }
            }
            bw.close();
            bwFrom.close();
            bwMention.close();
            bwHashtag.close();
            bwLocation.close();
            bwTerm.close();
            fileReaderA.close();
        }
        //TweetUtil.runScript("sort -t',' -rn -k3,3 " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights.csv > " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights1.csv");
        //TweetUtil.runScript("rm -f " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod+"/nonZero_featureWeights.csv");
        //TweetUtil.runScript("mv " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights1.csv > " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod+"/nonZero_featureWeights.csv");
    }

    public static void readNBResults(String clusterPath, String clusterOutPath, String featurePath, String topic, double nbrFeatures, String learningType) throws IOException, InterruptedException {
        String[] lambdaValues = {"0.1", "0.01", "0.001"};
        if(clusterPath.equals("")) {
            nbrFeatures = 1166582;
            clusterPath = "Data/LearningMethods/" + learningType + "/Topics/";
            clusterOutPath = "Data/LearningMethods/" + learningType + "/Topics/";
            featurePath = "Data/Learning/Topics/featureData/featureIndex.csv";
            topic = configRead.getGroupNames()[groupNum-1];
        }
        if(testFlag) {
            clusterPath = "TestSet/" + clusterPath;
            clusterOutPath = "TestSet/" + clusterOutPath;
            featurePath = "Data/test/Learning/Topics/featureData/featureIndex.csv";
            nbrFeatures = 1000;
        }
        outputCSVPath = clusterPath + topic + "/";
        String outputFilename = "model_"+topic+"_";
        SparkConf sparkConfig;
        sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        File folder1 = new File(outputCSVPath);
        ArrayList<String> fileNames1 = listFilesForFolder(folder1);
        BufferedWriter bw, bw2;
        FileWriter fw, fw2;
        DataFrame res  = null;
        String val;
        FileReader fileReaderA = new FileReader(featurePath);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        HashMap<Long, Double> featureMap = new HashMap<>();
        HashMap<String, Long> fromMap = new HashMap<>();
        HashMap<String, Long> mentionMap = new HashMap<>();
        HashMap<String, Long> termMap = new HashMap<>();
        HashMap<String, Long> hashtagsMap = new HashMap<>();
        HashMap<String, Long> locationMap = new HashMap<>();
        String line;
        int featCount = 0;
        String[] feats;
        int featureInd = 1, numberInd = 2;
        while ((line = bufferedReaderA.readLine()) != null) {
            featCount++;
            feats = line.split(",");
            featureMap.put(Long.valueOf(feats[numberInd]), 0.0);
            switch (feats[0]) {
                case "from":
                    fromMap.put(feats[featureInd], Long.valueOf(feats[numberInd]));
                    break;
                case "term":
                    termMap.put(feats[featureInd], Long.valueOf(feats[numberInd]));
                    break;
                case "hashtag":
                    hashtagsMap.put(feats[featureInd], Long.valueOf(feats[numberInd]));
                    break;
                case "mention":
                    mentionMap.put(feats[featureInd], Long.valueOf(feats[numberInd]));
                    break;
                case "location":
                    locationMap.put(feats[featureInd], Long.valueOf(feats[numberInd]));
                    break;
            }
        }
        bufferedReaderA.close();
        HashMap<String, Long> currentMap = null;
        for(String lambda : lambdaValues) {
            featureMap.clear();
            for(long j = 1; j <= featCount; j++)
                featureMap.put(j, 0.0);
            if(learningType.equals("Rocchio")) {
                lambda = "";
            }
            fw = new FileWriter(clusterOutPath + topic + "/" + outputFilename + lambda);
            bw = new BufferedWriter(fw);
            fw2 = new FileWriter(clusterOutPath + topic + "/" + outputFilename + lambda+"_features");
            bw2 = new BufferedWriter(fw2);
            bw.write("solver_type L2R_LR" + "\n");
            bw.write("nr_class 2" + "\n");
            bw.write("label 0 1" + "\n");
            bw.write("nr_feature "+nbrFeatures+" \n");
            bw.write("bias 1.000000000000000" + "\n");
            bw.write("w" + "\n");
            String type = "";
            double sum1 = 0, sum2 = 0;
            for (String filename1 : fileNames1) {
                if(!filename1.contains("_parquet")) continue;
                System.out.println("SUM1 : " + sum1);
                if(learningType.equals("NB") && !filename1.contains(lambda)) continue;
                res = sqlContext.read().parquet(outputCSVPath + "/" + filename1);
                if(filename1.contains("fromUser")) {
                    currentMap = fromMap;
                    type = "from:";
                }
                else if(filename1.contains("toUser")){
                    currentMap = mentionMap;
                    type = "mention:";
                }
                else if(filename1.contains("containTerm")){
                    currentMap = termMap;
                    type = "term:";
                }
                else if(filename1.contains("containHashtag")){
                    currentMap = hashtagsMap;
                    type = "hashtag:";
                }
                else if(filename1.contains("fromLocation")){
                    currentMap = locationMap;
                    type = "location:";
                }
                for(Row row : res.collectAsList()){
                    if(!currentMap.containsKey(row.getString(0))) {
                        System.out.printf(row.getString(0));
                        continue;
                    }
                    featureMap.put(currentMap.get(row.getString(0)), row.getDouble(1));
                    bw2.write(type + row.getString(0) + "," + String.format("%.16g ", row.getDouble(1)) + "\n");
                    sum1 += row.getDouble(1);
                }
            }
            int counter = 0;
            for(long i = 1; i <= featureMap.size(); i++){
                if(featureMap.get(i) != 0.0) {
                    bw.write(String.format("%.16g ", featureMap.get(i)));
                    sum2 += featureMap.get(i);
                }
//                    bw.write(new BigDecimal(featureMap.get(i)).toPlainString());
                else
                    bw.write("0 ");
                bw.write("\n");
            }
            bw.write("0 " + "\n");//for bias feature
            bw.close();
            bw2.close();
            if(learningType.equals("Rocchio"))
                break;
        }
        sparkContext.close();
    }

    private static void readNonzeroBaselineMixedWeights2() throws IOException, InterruptedException {
        boolean flag5000 = true;
        String logisticMethod;
        if(flag5000)
            logisticMethod = "learningFeatures5000";
        else
            logisticMethod = "learningFeatures";
        String path = "TestSet/Data/Data/Learning/Topics/";
        if(!testFlag)
            path = "ClusterResults/Nov27Res/Baselines/Out/";
        TweetUtil.runStringCommand("mkdir " + path);
        String outputPath = "ClusterResults/Nov27Res/";


        String[] algNames = new String[]{ "MI"};//"topical", "topicalLog", "MILog", "CP", "CPLog",

        for (String algName : algNames) {
            path = "ClusterResults/Nov27Res/Baselines/Out/" + algName + "/";
            TweetUtil.runStringCommand("mkdir " + path);
            path += "Data/";
            TweetUtil.runStringCommand("mkdir " + path);
            path+= "Learning/";
            TweetUtil.runStringCommand("mkdir " + path);
            path += "Topics/";
            TweetUtil.runStringCommand("mkdir " + path);
            TweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[groupNum - 1]);
            TweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/");
            TweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod);;
            FileReader fileReaderA = new FileReader(outputPath + "Baselines/" + configRead.getGroupNames()[groupNum - 1] + "/" + algName + "/Mixed"+((flag5000 == true)? 5000:"")+"/featureWeights.csv/part-00000");

            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
            String line;
            FileWriter fw = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featuresList_"+groupNum+".csv");
            BufferedWriter bw = new BufferedWriter(fw);
            FileWriter fw1 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_from.csv");
            BufferedWriter bwFrom = new BufferedWriter(fw1);
            FileWriter fw2 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_hashtag.csv");
            BufferedWriter bwHashtag = new BufferedWriter(fw2);
            FileWriter fw3 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_location.csv");
            BufferedWriter bwLocation = new BufferedWriter(fw3);
            FileWriter fw4 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_mention.csv");
            BufferedWriter bwMention = new BufferedWriter(fw4);
            FileWriter fw5 = new FileWriter(path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/featureWeights_term.csv");
            BufferedWriter bwTerm = new BufferedWriter(fw5);
            int counter = 1;
            while ((line = bufferedReaderA.readLine()) != null) {
                if (!line.split(",")[2].equals("0")) {
                    bw.write(line.split(",")[0].toLowerCase() + "," + line.split(",")[1]+ "," + counter +  "\n");
                    if (line.split(",")[0].equals("From"))
                        bwFrom.write(line.split(",")[1] + ",");
                    if (line.split(",")[0].equals("Hashtag"))
                        bwHashtag.write(line.split(",")[1] + ",");
                    if (line.split(",")[0].equals("Location"))
                        bwLocation.write(line.split(",")[1] + ",");
                    if (line.split(",")[0].equals("Mention"))
                        bwMention.write(line.split(",")[1] + ",");
                    if (line.split(",")[0].equals("Term"))
                        bwTerm.write(line.split(",")[1] + ",");
                    counter++;
                }
            }
            bw.close();
            bwFrom.close();
            bwMention.close();
            bwHashtag.close();
            bwLocation.close();
            bwTerm.close();
            fileReaderA.close();
        }
        //TweetUtil.runScript("sort -t',' -rn -k3,3 " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights.csv > " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights1.csv");
        //TweetUtil.runScript("rm -f " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod+"/nonZero_featureWeights.csv");
        //TweetUtil.runScript("mv " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod + "/nonZero_featureWeights1.csv > " + path + configRead.getGroupNames()[groupNum - 1] + "/fold0/" + logisticMethod+"/nonZero_featureWeights.csv");
    }

    public static void findTestTrainDataTids() throws IOException, InterruptedException {
        String path = "Data/Learning/Topics/";
        BufferedReader bufferedReader;
        FileReader fr;
        String line;
        String[] splits;
        FileWriter fw;
        BufferedWriter bw;
        int tweetCounter;
        for (int gNum = 1; gNum <= 10; gNum++){
            tweetCounter = 0;
            fr = new FileReader(path + "out_tweet_hashtag_user_mention_term_time_location_"+gNum+"_allInnerJoins_parquet_index.csv");
            tweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids");
            fw = new FileWriter(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tids.csv");
            bw = new BufferedWriter(fw);
            bufferedReader = new BufferedReader(fr);

            while ((line = bufferedReader.readLine()) != null) {
                splits = line.split(" ");
                bw.write(splits[splits.length-1] + "\n");
                tweetCounter++;
            }
            bw.close();
            bufferedReader.close();
            System.out.println(" Number of Tweets for " + configRead.getGroupNames()[gNum - 1] + " is : " + tweetCounter);
        }
    }

    public static void findTestTrainDataFeatures(SQLContext sqlContext) throws IOException, InterruptedException {
        DataFrame df;
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("hashtagGrouped", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        StructField[] f = {
                DataTypes.createStructField("topical", DataTypes.IntegerType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("term", DataTypes.StringType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true),
                DataTypes.createStructField("location", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true),
                DataTypes.createStructField("tid", DataTypes.LongType, true),
        };
        String path = "/Volumes/SocSensor/Zahra/ClusterResults_Oct29/TestTrainData/TestTrainDataCSV/";
        for (int gNum = 2; gNum <= 10; gNum++) {
            df = sqlContext.read().parquet("/Volumes/SocSensor/Zahra/ClusterResults_Oct29/TestTrainData/" + "tweet_hashtag_user_mention_term_time_location_" + gNum + "_allInnerJoins_parquet");
            /*df = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(path +"out_tweet_hashtag_user_mention_term_time_location_1_allInnerJoins_parquet_csvFormat.csv" ).javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
                @Override
                public Iterable<Row> call(Row v1) throws Exception {
                    String h = "", term = "", mention = "", username = "", location = "";
                    username = v1.getString(1).split(":")[1];
                    for(String s: v1.getString(2).split(":")){
                        if(s.equals("hashtag")) continue;
                        h += s.split(" ")[0] + ",";
                    }
                    h = h.substring(0, h.length()-1).toLowerCase();
                    for(String s: v1.getString(3).split(":")){
                        if(s.equals("term")) continue;
                        term += s.split(" ")[0] + ",";
                    }
                    term = term.substring(0, term.length()-1).toLowerCase();
                    for(String s: v1.getString(4).split(":")){
                        if(s.equals("mention")) continue;
                        mention += s.split(" ")[0] + ",";
                    }
                    mention = mention.substring(0, mention.length()-1).toLowerCase();
                    location = v1.getString(5).split(":")[1];
                    List<Row> list = new ArrayList<Row>();

                        for(String _term : term.split(",")){
                            for(String _mention : mention.split(","))
                                list.add(RowFactory.create(Integer.valueOf(v1.getString(0)),username, h, _term, _mention,
                                        location, Long.valueOf(v1.getString(6)), Long.valueOf(v1.getString(7))));
                        }
                    return list;
                }
            }), new StructType(f));*/
            System.out.println(df.count());
            df.select("tid", "username", "hashtag", "time").distinct().write().mode(SaveMode.Overwrite).parquet(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_user_hashtag_grouped_parquet");
            df.select("tid", "mentionee", "hashtag", "time").distinct().write().mode(SaveMode.Overwrite).parquet(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_mention_hashtag_grouped_parquet");
            df.select("tid", "location", "hashtag", "time").distinct().write().mode(SaveMode.Overwrite).parquet(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_location_hashtag_grouped_parquet");
            df.select("tid", "term", "hashtag", "time").distinct().write().mode(SaveMode.Overwrite).parquet(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_term_hashtag_grouped_parquet");
            sqlContext.createDataFrame(df.select("tid", "hashtag", "time").distinct().javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
                @Override
                public Iterable<Row> call(Row row) throws Exception {
                    List<Row> list = new ArrayList<Row>();
                    if(row.get(1) == null)
                        return list;
                    String[] splits = row.getString(1).split(",");
                    for (String s : splits)
                        list.add(RowFactory.create(row.getLong(0), s, row.getString(1), row.getLong(2)));
                    return list;
                }
            }), new StructType(fields)).write().mode(SaveMode.Overwrite).parquet(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_hashtag_hashtag_grouped_parquet");
        }

/*
        BufferedReader bufferedReader;
        FileReader fr;
        String line;
        String[] splits, splits2;
        FileWriter fwUser, fwLoc, fwMention, fwHashtag, fwTerm;
        BufferedWriter bwUser, bwLoc, bwMention, bwHashtag, bwTerm;
        int tweetCounter;
        final String emo_regex2 = "\\([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee]\\)";//"\\p{InEmoticons}";
        for (int gNum = 1; gNum <= 10; gNum++){
            tweetCounter = 0;
            fr = new FileReader("/Volumes/SocSensor/Zahra/ClusterResults_Oct29/TestTrainDataCSV/" + "out_tweet_hashtag_user_mention_term_time_location_"+gNum+"_allInnerJoins_parquet.csv");
            tweetUtil.runStringCommand("mkdir " + path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids");
            fwUser = new FileWriter(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_user_hashtag_grouped.csv");
            bwUser = new BufferedWriter(fwUser);
            fwLoc = new FileWriter(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_location_hashtag_grouped.csv");
            bwLoc = new BufferedWriter(fwLoc);
            fwMention = new FileWriter(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_mention_hashtag_grouped.csv");
            bwMention = new BufferedWriter(fwMention);
            fwHashtag = new FileWriter(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_hashtag_hashtag_grouped.csv");
            bwHashtag = new BufferedWriter(fwHashtag);
            fwTerm = new FileWriter(path + configRead.getGroupNames()[gNum - 1] + "/fold0/Ids/tweet_term_hashtag_grouped.csv");
            bwTerm = new BufferedWriter(fwTerm);
            bufferedReader = new BufferedReader(fr);
            long tid, time;
            String str, feature, hashtag;
            while ((line = bufferedReader.readLine()) != null) {
                hashtag = "";
                splits2 = line.split(" hashtag:");
                if(splits2.length > 1) {
                    for (int j = 1; j < splits2.length; j += 2) {
                        hashtag += splits2[j];
                    }
                }
                splits = line.split(" ");
                tid = Long.valueOf(splits[splits.length - 1]);
                time = Long.valueOf(splits[splits.length-2]);
                for(int i = 1; i < splits.length-2; i++) {
                    str = splits[i];
                    feature = str.split(":")[1];
                    switch (str.split(":")[0]) {
                        case "from":
                            bwUser.write(tid + "," + feature + "," + hashtag + "," + time + "\n");
                            break;
                        case "term":
                            bwTerm.write(tid + "," + feature + "," + hashtag + "," + time + "\n");
                            break;
                        case "hashtag":
                            bwHashtag.write(tid + "," + feature + "," + hashtag + "," + time + "\n");
                            break;
                        case "mention":
                            bwMention.write(tid + "," + feature + "," + hashtag + "," + time + "\n");
                            break;
                        case "location":
                            Matcher matcher = Pattern.compile(emo_regex2).matcher(feature);
                            feature = matcher.replaceAll("").trim();
                            feature = feature.toLowerCase().replace(" ", "");
                            bwLoc.write(tid + "," + feature + "," + hashtag + "," + time + "\n");
                            break;
                    }
                }
                tweetCounter++;
            }
            bwUser.close(); bwLoc.close(); bwMention.close(); bwHashtag.close(); bwTerm.close();
            bufferedReader.close();
            System.out.println(" Number of Tweets for " + configRead.getGroupNames()[gNum - 1] + " is : " + tweetCounter);
        }*/
    }

    public static void writeTableTopFeatureTopics() throws IOException {
        String path = "/Volumes/SocSensor/Zahra/FeatureAnalysisICWSM/FeatureAnalysis_TopMiddle/";
        String featureAlg = "MI";
        String line;
        FileReader fileReader;
        BufferedReader bufferedReader;
        int classInd, topInd;
        String[][] topsFeature = new String[configRead.getNumOfGroups()][10];
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path + "finalTopTable.csv"));
        bufferedWriter.write("top10,");
        for(String topic: configRead.getGroupNames()) {
            bufferedWriter.write(topic  + ",");
        }
        bufferedWriter.write("\n");
        for(String featureType : new String[]{"From", "Hashtag", "Location", "Mention", "Term"}) {
            classInd = 0;
            for(String topic: configRead.getGroupNames()){
                topInd = 0;
                System.out.println(path + topic + "/" + featureAlg + "/" + featureType + "/top10_" + featureType + "1.csv");
                fileReader = new FileReader(path + topic + "/" + featureAlg + "/" + featureType + "/top10_" + featureType + "1.csv");
                bufferedReader = new BufferedReader(fileReader);
                while((line = bufferedReader.readLine()) != null){
                    topsFeature[classInd][topInd] = line.split(",")[0];
                    topInd++;
                }
                classInd++;
            }
            for(int i = 0; i < 10; i++){
                bufferedWriter.write(featureType + ",");
                for(int j = 0; j < configRead.getNumOfGroups()-1; j++) {
                    bufferedWriter.write(topsFeature[j][i] + ",");
                }
                bufferedWriter.write(topsFeature[configRead.getNumOfGroups()-1][i] + "\n");
            }
            bufferedWriter.write("\n");
            for(int i = 0; i < 10; i++){
                for(int j = 0; j < configRead.getNumOfGroups()-1; j++) {
                    topsFeature[i][j] = "";
                }
            }
            bufferedWriter.write("\n");
        }
        bufferedWriter.close();
    }

    public static void getTweetMonthStats(SQLContext sqlContext) throws ParseException {
        StructField[] fields = {
                DataTypes.createStructField("monthNumber", DataTypes.IntegerType, true)
        };
        String path = "";
        DataFrame tweetTime = sqlContext.read().parquet(path + "tweet_time_parquet");
        final long[] months = new long[24];
        int ind = 0;

        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        months[ind] = format.parse("Tue Jan 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Fri Feb 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Fri Mar 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Mon Apr 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Wed May 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Sat Jun 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Mon Jul 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Thu Aug 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Sun Sept 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Tue Oct 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Fri Nov 1 00:00:00 +0000 2013").getTime();
        ind++;
        months[ind] = format.parse("Sun Dec 1 00:00:00 +0000 2013").getTime();
        ind++;

        months[ind] = format.parse("Wed Jan 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Sat Feb 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Sat Mar 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Tue Apr 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Thu May 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Sun Jun 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Tue Jul 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Fri Aug 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Mon Sept 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Wed Oct 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Sat Nov 1 00:00:00 +0000 2014").getTime();
        ind++;
        months[ind] = format.parse("Mon Dec 1 00:00:00 +0000 2014").getTime();
        ind++;

        DataFrame monthCounts = sqlContext.createDataFrame(tweetTime.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                int index = 0;
                while (v1.getLong(1) < months[index])
                    index++;
                return RowFactory.create(index - 1);
            }
        }), new StructType(fields));
        monthCounts.write().format("com.databricks.spark.csv").save(path + "monthNumbers_csv");
    }

    public static void analyzeMI() throws IOException {
        //String userFeatureCounts = "irandeal-From_favoriteCount_MI.csv";
        String commonPath =  "/Users/zahraiman/University/FriendSensor/SPARK/SocialSensorProject_oct7/socialsensor/Data/MIAnalysis/";
        //String[] countNames = {"allMI_CountfTtT.csv", "allMI_CountfTtF.csv", "allMI_CountfFtT.csv", "allMI_CountfFtF.csv"};

        String countsPath = "/Volumes/SocSensor/Zahra/MIAnalysis/";
        String line;
        String[] splits;
        HashSet<String> namesGreater = new HashSet<>();
        FileReader fileReaderA = new FileReader(countsPath + "namesGreater2E9.csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);

        while ((line = bufferedReaderA.readLine()) != null) {
            namesGreater.add(line);
        }
        bufferedReaderA.close();
        BufferedReader bufferedReaderTT = new BufferedReader(new FileReader(countsPath + "allMI_CountfTtT.csv"));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(commonPath + "ProbsGreater_allMI_CountfTtT.csv"));
        while ((line = bufferedReaderTT.readLine()) != null) {
            splits = line.split(",");
            if(splits.length < 2)
                continue;
            if(namesGreater.contains(splits[0]))
                bufferedWriter.write(splits[1] + "\n");
        }
        bufferedWriter.close();
        bufferedReaderTT.close();
        BufferedReader bufferedReaderTF = new BufferedReader(new FileReader(countsPath + "allMI_CountfTtF.csv"));
        bufferedWriter = new BufferedWriter(new FileWriter(commonPath + "ProbsGreater_allMI_CountfTtF.csv"));
        while ((line = bufferedReaderTF.readLine()) != null) {
            splits = line.split(",");
            if(splits.length < 2)
                continue;
            if(namesGreater.contains(splits[0]))
                bufferedWriter.write(splits[1] + "\n");
        }
        bufferedWriter.close();
        bufferedReaderTF.close();

        BufferedReader bufferedReaderFT = new BufferedReader(new FileReader(countsPath + "allMI_CountfFtT.csv"));
        bufferedWriter = new BufferedWriter(new FileWriter(commonPath + "ProbsGreater_allMI_CountfFtT.csv"));
        while ((line = bufferedReaderFT.readLine()) != null) {
            splits = line.split(",");
            if(splits.length < 2)
                continue;
            if(namesGreater.contains(splits[0]))
                bufferedWriter.write(splits[1] + "\n");
        }
        bufferedWriter.close();
        bufferedReaderFT.close();

        BufferedReader bufferedReaderFF = new BufferedReader(new FileReader(countsPath + "allMI_CountfFtF.csv"));
        bufferedWriter = new BufferedWriter(new FileWriter(commonPath + "ProbsGreater_allMI_CountfFtF.csv"));
        while ((line = bufferedReaderFF.readLine()) != null) {
            splits = line.split(",");
            if(splits.length < 2)
                continue;
            if(namesGreater.contains(splits[0]))
                bufferedWriter.write(splits[1] + "\n");
        }
        bufferedWriter.close();
        bufferedReaderFF.close();
    }
}














