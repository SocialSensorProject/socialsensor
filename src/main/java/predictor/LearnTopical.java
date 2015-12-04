package predictor;

import predictor.de.bwaldvogel.liblinear.InvalidInputDataException;
import predictor.de.bwaldvogel.liblinear.Predict;
import predictor.de.bwaldvogel.liblinear.Train;
import preprocess.spark.ConfigRead;
import util.Statistics;
import util.TweetUtil;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by imanz on 9/24/15.
 */
public class LearnTopical {
    private static Map<String, Long> hashtagMap;
    private static Map<String, Long> indexMap;
    private static DecimalFormat df3 = new DecimalFormat("#.###");
    private static int featureNum = 1000000;
    private static int sampleNum = 2000000;
    private static TweetUtil tweetUtil;

    private static String path = "Data/Learning/Topics/";
    private static String LRPath = "Data/Learning/LogisticRegression/";
    private static String featurepath = "featureData/";
    private static String hashtagFileName = "hashtagIndex";
    private static String indexFileName = "featureIndex";
    private static String allHashtagList = "allHashtag";
    private static String hashtagSetDate = "hashtagSet_Date.csv";
    private static String testHashtagList = "testHashtagList";
    private static String trainHashtagList = "trainHashtagList";
    private static String trainFileName = "testTrain_train_";
    private static String testFileName = "testTrain_test_";
    private static String outputFileName = "output_disaster";
    private static String modelFileName = "model_disaster";
    private static String solverType;
    private static int numOfFolds = 1;
    private static int numOfTopics;
    private static String[] classNames;
    private static int[][] positives;
    private static int[][] total;
    private static int[][] positivesVal;
    private static int[][] totalVal;
    private static ConfigRead configRead;
    private static boolean testFlag = false;
    private static Map<Integer, String> invFeatures;
    private static double percentageTrain = 0.5;
    private static double percentageVal = 0.6;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }


    /*
     * Run tests on data
     */
    public static void main(String[] args) throws IOException, InvalidInputDataException, ParseException, InterruptedException {
        loadConfig();
        testFlag = configRead.getTestFlag();
        if(configRead.getTrainPercentage() == 0.7){
            percentageTrain = 0.7;
            percentageVal = 0.8;
        }
        tweetUtil = new TweetUtil();
        if(testFlag){
            path = "Data/test/Learning/Topics/";
            LRPath = "Data/test/Learning/LogisticRegression/";
            classNames = new String[] {"naturaldisaster"};
            numOfTopics = 1;
        }else{
            numOfTopics = configRead.getNumOfGroups();
            classNames = configRead.getGroupNames();
        }
        positives = new int[classNames.length][numOfFolds];
        total = new int[classNames.length][numOfFolds];
        positivesVal = new int[classNames.length][numOfFolds];
        totalVal = new int[classNames.length][numOfFolds];
        String time1 = "2013-06-20 15:08:01";
        String time2 = "Thu Jun 20 15:08:01 +0001 2013";
        long t = new SimpleDateFormat("yyy-MM-dd HH':'mm':'ss").parse(time1).getTime();
        long t2 = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse(time2).getTime();
        boolean filePrepare = true;

        if (filePrepare) {
            for(String classname : classNames) {
                tweetUtil.runStringCommand("mkdir " + path + classname);
                tweetUtil.runStringCommand("mkdir " + LRPath);
                tweetUtil.runStringCommand("mkdir " + LRPath + classname);
                for (int ifold = 0; ifold < numOfFolds; ifold++) {
                    tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + ifold);
                    tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + ifold + "/l2_lr");
                    tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + ifold + "/l1_lr");
                    tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + ifold + "/l2_lrd");
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr");
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr" + "/" + "fold" + ifold);
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr" + "/" + "fold" + ifold + "/bestc");
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr");
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr" + "/" + "fold" + ifold);
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr" + "/" + "fold" + ifold + "/bestc");
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd");
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd" + "/" + "fold" + ifold);
                    tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd" + "/" + "fold" + ifold + "/bestc");
                }
            }
            prepareTestTrainSplits();
            int classInd=0;
            for(String classname : classNames) {
                classInd++;
                if(classInd != 1)// && classInd!= 7)
                    continue;
                //modifyFeatureList();
                findTestTrain(classInd);
                findTopicalTest(trainFileName, trainHashtagList, classname, classInd-1);
                findTopicalTest(testFileName, testHashtagList, classname, classInd-1);
                findTopicalTest(trainFileName + "_t", trainHashtagList + "_t", classname, classInd-1);
                findTopicalTest(trainFileName + "_v", trainHashtagList + "_v", classname, classInd-1);
            }
        }

        ArrayList<Double> accuracies = new ArrayList<Double>();
        ArrayList<Double> precisions = new ArrayList<Double>();
        ArrayList<Double> recalls = new ArrayList<Double>();
        ArrayList<Double> fscores = new ArrayList<Double>();
        solverType = "l2_lr";
        //solverType = "l2_lrd";
        //solverType = "l1_lr";

        FileWriter fw = new FileWriter(path +"learning_Info_"+solverType+".csv");
        BufferedWriter bw = new BufferedWriter(fw);



        Train train = new Train();
        String[] arguments = new String[50];
        Predict predict = new Predict();
        String[] argumentsPred = new String[50];
        int ind = 0;
        int predInd = 0;


        //arguments[ind] = "-v";ind++;
        //arguments[ind] = "10";ind++;
        arguments[ind] = "-s";
        ind++;
        if (solverType.equals("l2_lr"))
            arguments[ind] = "0";
        else if (solverType.equals("l1_lr"))
            arguments[ind] = "6";
        else if (solverType.equals("l2_lrd"))
            arguments[ind] = "7";
        ind++;
        arguments[ind] = "-B";
        ind++;
        arguments[ind] = "1";
        ind++;

        argumentsPred[predInd] = "-b";
        predInd++;
        argumentsPred[predInd] = "1";
        predInd++;
        int remInd = ind, remPredInd = predInd;
        //System.out.println("Running " + getName() + " using " + source_file);
        double[] cValues = {1e-10, 1e-8, 1e-9, 1e-7, 1e-6, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 1e4, 1e5, 1e7, 1e10};
        //double[] cValues = {1e-5, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 1e7};
        double bestc = -1, bestError = -1;
        int classInd = -1;
        //for (int classname = 1; classname <= numOfTopics; classname++) {
        double d;
        for (String classname : classNames) {
            bw.write("================================ " + classname + " ============================\n");
            classInd++;
            if(classInd != 0)// && classInd != 6)
                continue;
            accuracies = new ArrayList<Double>();
            precisions = new ArrayList<Double>();
            recalls = new ArrayList<Double>();
            fscores = new ArrayList<Double>();

            System.out.println("========================TopicNum: " + classname + "============================");
            for (int i = 0; i < numOfFolds; i++) {
                bestc = -1;
                bestError = -1;
                System.out.println("========================foldNum: " + i + "============================");
                String trainName = classname + "/fold" + i + "/" + trainFileName + "_t.csv";
                String testName = classname + "/fold" + i + "/" + trainFileName + "_v.csv";
                //String trainName = classname + "/fold" + i + "/" + trainFileName + ".csv";
                //String testName = classname + "/fold" + i + "/" + testFileName + ".csv";
                for (double c : cValues) {
                    ind = remInd;
                    predInd = remPredInd;
                    System.out.println("========================C Value: " + c + "============================");
                    arguments[ind] = "-w0";ind++;
                    arguments[ind] = String.valueOf(c);ind++;
                    arguments[ind] = "-w1";ind++;
                    //arguments[ind] = String.valueOf(c);ind++;
                    //d = ((double)total[classInd][i]-positives[classInd][i])/positives[classInd][i];
                    d = (double)((total[classInd][i]+totalVal[classInd][i])-(positives[classInd][i]+positivesVal[classInd][i]))/(positives[classInd][i]+positivesVal[classInd][i]);
                    //d  = 1.0;
                    arguments[ind] = String.valueOf(c*d);ind++;
                    arguments[ind] = path + trainName;
                    ind++;
                    arguments[ind] = LRPath + classname + "/" + solverType + "/fold" + i + "/" + modelFileName + "_" + c;
                    ind++;
                    Arrays.copyOfRange(arguments, 0, ind - 1);
                    train.run(arguments);

                    argumentsPred[predInd] = path + testName;
                    predInd++;
                    argumentsPred[predInd] = LRPath + classname + "/" + solverType + "/fold" + i + "/" + modelFileName + "_" + c;
                    predInd++;
                    argumentsPred[predInd] = LRPath + classname + "/" + solverType + "/fold" + i + "/" + outputFileName + "_" + c;
                    predInd++;
                    Arrays.copyOfRange(argumentsPred, 0, predInd - 1);

                    double[] measures = predict.mainPredict(argumentsPred);
                    //accuracies.add(measures[0]);
                    //precisions.add(measures[1]);
                    //recalls.add(measures[2]);
                    //fscores.add(measures[3]);
                    //if (measures[3] > bestError) { //error
                    if(measures[0] > bestError){
                        bestc = c;
                        bestError = measures[0];
                    }
                    bw.write("C value: " + c + " accuracy: " + df3.format(measures[0]) + " - precision: " + df3.format(measures[1]) + " - recall: " + df3.format(measures[2]) + " - f-score: "+ df3.format(measures[3]) + "\n");
                }
                //bestc = 1e-6;
                System.err.println(" For classname: " + classname + " and foldNum: " + i + " , the best C is : " + bestc + " with F-Score value of " + bestError);
                //Evaluate on Test with bestc found on train validation data
                testName = classname + "/fold" + i + "/" + testFileName + ".csv";//  + (i+1);
                trainName = classname + "/fold" + i + "/" + trainFileName + ".csv";//  + (i+1);
                double c = bestc;
                predInd = remPredInd;
                System.out.println("========================Evaluate on Test data with C Value: " + c + "============================");
                ind = remInd;
                arguments[ind] = "-w0";ind++;
                arguments[ind] = String.valueOf(c);ind++;
                arguments[ind] = "-w1";ind++;
                //arguments[ind] = String.valueOf(c);ind++;
                d = ((total[classInd][i]+totalVal[classInd][i])-(positives[classInd][i]+positivesVal[classInd][i]))/(positives[classInd][i]+positivesVal[classInd][i]);
                //d  = 1.0;
                arguments[ind] = String.valueOf(c*d);ind++;
                arguments[ind] = path + trainName;
                ind++;
                arguments[ind] = LRPath + classname + "/" + solverType + "/fold" + i + "/bestc/" + modelFileName + "_" + c;
                ind++;
                Arrays.copyOfRange(arguments, 0, ind - 1);
                train.run(arguments);
                argumentsPred[predInd] = path + testName;
                predInd++;
                argumentsPred[predInd] = LRPath + classname + "/" + solverType + "/fold" + i + "/bestc/" + modelFileName + "_" + c;
                predInd++;
                argumentsPred[predInd] = LRPath + classname + "/" + solverType + "/fold" + i + "/bestc/" + outputFileName + "_" + c;
                predInd++;
                Arrays.copyOfRange(argumentsPred, 0, predInd - 1);
                double[] measures = predict.mainPredict(argumentsPred);
                accuracies.add(measures[0]);
                precisions.add(measures[1]);
                recalls.add(measures[2]);
                fscores.add(measures[3]);

                writeFeatureFile(classname, LRPath + classname + "/" + solverType + "/fold" + i + "/bestc/" + modelFileName + "_" + c, classInd+1);
                bw.write("****** TEST DATA with C value: " + c + " accuracy: " + df3.format(measures[0]) + " - precision: " + df3.format(measures[1]) + " - recall: " + df3.format(measures[2]) + " - f-score: " + df3.format(measures[3]) + "\n");
                bw.flush();
            }

            for (int o = 0; o < accuracies.size(); o++) {
                System.out.println(accuracies.get(o) + " " + precisions.get(o) + " " + recalls.get(o) + " " + fscores.get(o));
            }

            //System.out.println("- Finished fold " + (i+1) + ", accuracy: " + df3.format( correct / (double)_testData._data.size() ));
            System.out.println("Accuracy:  " + df3.format(Statistics.Avg(accuracies)) + "  +/-  " + df3.format(Statistics.StdError95(accuracies)));
            System.out.println("Precision: " + df3.format(Statistics.Avg(precisions)) + "  +/-  " + df3.format(Statistics.StdError95(precisions)));
            System.out.println("Recall:    " + df3.format(Statistics.Avg(recalls)) + "  +/-  " + df3.format(Statistics.StdError95(recalls)));
            System.out.println("F-Score:   " + df3.format(Statistics.Avg(fscores)) + "  +/-  " + df3.format(Statistics.StdError95(fscores)));
            System.out.println();
        }
        bw.close();
    }
    public static void writeFeatureFile(String classname, String modelName, int groupNum) throws IOException, InterruptedException {

        //build test/train data and hashtag lists
        for (int i = 0; i < numOfFolds; i++) {
            FileReader fileReaderA = new FileReader(modelName);
            BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
            FileReader fileReaderB = new FileReader(path + featurepath + indexFileName + "_" + groupNum+".csv");
            BufferedReader bufferedReaderB = new BufferedReader(fileReaderB);
            FileWriter fw = new FileWriter(path + classname +"/fold" + i +  "/" + solverType + "/featureWeights.csv");
            BufferedWriter bw = new BufferedWriter(fw);
            String line = "", line2;String [] splits;int ind = 0;
            for(int kk = 0; kk < 6; kk++)//read header
                line = bufferedReaderA.readLine();
            List<String> featureWeights = new ArrayList<>();
            while ((line = bufferedReaderA.readLine()) != null) {//last line of model is the bias feature
                featureWeights.add(new BigDecimal(Double.valueOf(line)).toPlainString());
            }
            for(int ik = 0; ik < featureWeights.size()-1; ik++) {
                line2 = bufferedReaderB.readLine();
                ind++;
                splits = line2.split(",");
                bw.write(splits[0].toLowerCase() + "," + splits[1].toLowerCase() + "," + featureWeights.get(ik) + "\n");
            }
            fileReaderA.close();
            fileReaderB.close();
            bw.close();
            tweetUtil.runStringCommand("sort -t',' -rn -k3,3 " + path + classname + "/fold" + i + "/" + solverType + "/featureWeights.csv > " + path + classname + "/fold" + i + "/" + solverType + "/featureWeights1.csv");
            tweetUtil.runStringCommand("rm -rf " + path + classname + "/fold" + i + "/" + solverType + "/featureWeights.csv");
            tweetUtil.runStringCommand("mv " + path + classname + "/fold" + i + "/" + solverType + "/featureWeights1.csv " + path + classname + "/fold" + i + "/" + solverType + "/featureWeights.csv");
        }
    }

    public static String getFeatureNames(String featureLine, int groupNum) throws IOException {
        String[] splits;
        if(invFeatures == null) {
            FileReader fileReaderB = new FileReader(path + featurepath + indexFileName + "_" + groupNum + ".csv");
            BufferedReader bufferedReaderB = new BufferedReader(fileReaderB);
            invFeatures = new HashMap<>();
            String line;

            while ((line = bufferedReaderB.readLine()) != null) {
                splits = line.split(",");
                if(splits.length == 2)
                    invFeatures.put(Integer.valueOf(splits[1]), splits[0]);
                else
                    invFeatures.put(Integer.valueOf(splits[2]), splits[0]+":"+splits[1]);
            }
            bufferedReaderB.close();
        }

        //build test/train data and hashtag lists
        splits = featureLine.split(" ");
        String out = splits[0];
        for(int i = 1; i < splits.length-2; i++) {
            out += " " + invFeatures.get(Integer.valueOf(splits[i].split(":")[0]));
        }
        out+= " " + splits[splits.length-2];
        out+= " " + splits[splits.length-1];
        return out;
    }

    /*
    Prepare temporal splits for test and train and cross-validataion
     */
    public static void prepareTestTrainSplits() throws ParseException, IOException, InterruptedException {

        long []splitTimestamps = new long[numOfFolds];
        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        /*String[] dates = {"Sun Sep 01 00:00:00 +0000 2013", "Tue Oct 01 00:00:00 +0000 2013", "Fri Nov 01 00:00:00 +0000 2013",
                "Sun Dec 01 00:00:00 +0000 2013", "Wed Jan 01 00:00:00 +0000 2014", "Sat Feb 01 00:00:00 +0000 2014",
                "Sat Mar 01 00:00:00 +0000 2014", "Tue Apr 01 00:00:00 +0000 2014", "Thu May 01  00:00:00 +0000 2014",
                "Sun Jun 01 00:00:00 +0000 2014"};
                String dates0 = "Thu Aug 01 00:00:00 +0000 2013";
        */
        /*String[] dates = {"Wed Jan 01 00:00:00 +0000 2014", "Sat Feb 01 00:00:00 +0000 2014",
                "Sat Mar 01 00:00:00 +0000 2014", "Tue Apr 01 00:00:00 +0000 2014", "Thu May 01  00:00:00 +0000 2014",
                "Sun Jun 01 00:00:00 +0000 2014", "Tue Jul 01 00:00:00 +0000 2014", "Fri Aug 01 00:00:00 +0000 2014", "Mon Sep 01 00:00:00 +0000 2014", "Wed Oct 01 00:00:00 +0000 2014"};
        //"Mon Jul 01 00:00:00 +0000 2013",
        String[] valDates = {"Fri Nov 01 00:00:00 +0000 2013",
                "Sun Dec 01 00:00:00 +0000 2013", "Wed Jan 01 00:00:00 +0000 2014", "Sat Feb 01 00:00:00 +0000 2014",
                "Sat Mar 01 00:00:00 +0000 2014", "Tue Apr 01 00:00:00 +0000 2014", "Thu May 01  00:00:00 +0000 2014", "Sun Jun 01 00:00:00 +0000 2014", "Tue Jul 01 00:00:00 +0000 2014", "Fri Aug 01 00:00:00 +0000 2014"};
        String dates0 = "Sun Dec 01 00:00:00 +0000 2013";*/
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        FileWriter fw, fwTest, fwVal, fwAllTrain;
        BufferedWriter bw, bwTest, bwVal, bwAllTrain;
        FileWriter fwName, fwTestName, fwValName, fwAllTrainName;
        BufferedWriter bwName, bwTestName, bwValName, bwAllTrainName;
        String [] splitSt; String classFileName = "";
        int trainFileSize = 0,testFileSize = 0, trainValFileSize = 0; long valSplit;

        //make a hashmap of hashtag_dates of all topical hashtags
        String line;
        fileReaderA = new FileReader(path +featurepath + hashtagSetDate);
        bufferedReaderA = new BufferedReader(fileReaderA);
        Map<String, Long> hashtagDate = new HashMap<>();
        Map<String, Long> featureMap;
        while ((line = bufferedReaderA.readLine()) != null) {
            splitSt = line.split(",");
            hashtagDate.put(splitSt[0].toLowerCase(), Long.valueOf(splitSt[1]));
        }
        bufferedReaderA.close();

        //build test/train data and hashtag lists
        int classInd = -1, firstLabel;
        List<String[]> splitDates;

        int tweets2014Num = 0;long cDate;
        for(String classname : classNames) {
            System.out.println("==============================="+classname+"=============================");
            classInd++;
            if(classInd != 0)// && classInd != 6)
                continue;
            featureMap = new HashMap<>();
            fileReaderA = new FileReader(path +featurepath + indexFileName + "_"+(classInd+1)+".csv");
            bufferedReaderA = new BufferedReader(fileReaderA);

            while ((line = bufferedReaderA.readLine()) != null) {
                splitSt = line.split(",");
                if(splitSt[0].toLowerCase().equals("hashtag"))
                    featureMap.put(splitSt[1].toLowerCase(), Long.valueOf(splitSt[2]));
            }
            bufferedReaderA.close();


            fw = new FileWriter(path + classNames[classInd] + "/" +"allHashtag_"+classNames[classInd]+".csv");
            bw = new BufferedWriter(fw);
            HashMap<String, Long> hashtagSetDate = new HashMap<String, Long>();
            for(String s: tweetUtil.getGroupHashtagList(classInd+1, testFlag)){
                if (hashtagDate.containsKey(s)){// && featureMap.containsKey(s)) {
                    hashtagSetDate.put(s, hashtagDate.get(s));
                    bw.write(s + "\n");
                }else
                    System.out.println("ERROR: " + s);
            }
            bw.close();

            String[] splitDatesStr = null;
            if(!testFlag)
                splitDatesStr = findSplitDates(hashtagSetDate, classInd+1);
            else {
                splitDates = new ArrayList<>();
                splitDates.add(new String[]{String.valueOf(format.parse("Wed Nov 20 14:08:01 +0001 2013").getTime()), String.valueOf(format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime())});
                splitDatesStr = splitDates.get(0);
            }

            if(classInd != 0)// && classInd != 6)
                continue;
            for (int i = 0; i < numOfFolds; i++) {
                tweets2014Num = 0;
                firstLabel = -1;
                trainFileSize = 0;testFileSize = 0;trainValFileSize = 0;
                //splitTimestamps[i] = format.parse(splitDates.get(classInd)[0]).getTime();
                splitTimestamps[i] = Long.valueOf(splitDatesStr[1]);
//                tweetUtil.runStringCommand("perl -MList::Util -e 'print List::Util::shuffle <>' " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet_all.csv" + " > " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet_all1.csv");
//                tweetUtil.runStringCommand("rm -f " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet_all.csv");
//                tweetUtil.runStringCommand("mv " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet_all1.csv" + " " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet_all.csv");
                fileReaderA = new FileReader(path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_allTrainData_parquet_all.csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                fw = new FileWriter(path + classname + "/fold" + i + "/" + trainFileName+ "_t.csv");
                bw = new BufferedWriter(fw);
                fwTest = new FileWriter(path + classname + "/fold" + i + "/" +  testFileName  + ".csv");
                bwTest = new BufferedWriter(fwTest);
                fwVal = new FileWriter(path + classname + "/fold" + i + "/" + trainFileName  + "_v.csv");
                bwVal = new BufferedWriter(fwVal);
                fwAllTrain = new FileWriter(path + classname + "/fold" + i + "/" +  trainFileName + ".csv");
                bwAllTrain = new BufferedWriter(fwAllTrain);
                /*fwName = new FileWriter(path + classname + "/fold" + i + "/" + trainFileName + "_t_strings.csv");
                bwName = new BufferedWriter(fwName);
                fwValName = new FileWriter(path + classname + "/fold" + i + "/" + trainFileName + "_v_strings.csv");
                bwValName = new BufferedWriter(fwValName);
                fwAllTrainName = new FileWriter(path + classname + "/fold" + i + "/" + trainFileName + "_strings.csv");
                bwAllTrainName = new BufferedWriter(fwAllTrainName);
                fwTestName = new FileWriter(path + classname + "/fold" + i + "/" + testFileName + "_strings.csv");
                bwTestName = new BufferedWriter(fwTestName);*/
                //WRITE THE HASHTAG LIST BASED ON TIMESTAMP
                String cleanLine = "", lineName;
                while ((line = bufferedReaderA.readLine()) != null) {
                    //lineName = getFeatureNames(line);
                    splitSt = line.split(" ");
                    cleanLine = splitSt[0];
                    for (int j = 1; j < splitSt.length - 2; j++) {
                        cleanLine += " " + splitSt[j];
                    }
                    cDate = Long.valueOf(splitSt[splitSt.length - 2]);
                    if(cDate > 1388534339000l)
                        tweets2014Num++;
                    if (cDate <= splitTimestamps[i]) {
                        if(cDate >= Long.valueOf(splitDatesStr[0])){
                            bwVal.write(cleanLine + "\n");
                            //bwValName.write(lineName + "\n");
                            trainValFileSize++;
                        }else {
                            trainFileSize++;
                            bw.write(cleanLine + "\n");
                            //bwName.write(lineName + "\n");
                        }
                        bwAllTrain.write(cleanLine + "\n");
                        //bwAllTrainName.write(lineName + "\n");
                    }
                    else {
                        testFileSize++;
                        bwTest.write(cleanLine + "\n");
                        //bwTestName.write(lineName + "\n");
                    }
                }
                bufferedReaderA.close();
                bw.close();
                bwTest.close();
                bwVal.close();
                bwAllTrain.close();
                /*bwName.close();
                bwTestName.close();
                bwValName.close();
                bwAllTrainName.close();*/
                int totSize = trainFileSize+trainValFileSize+testFileSize;
                System.out.println("FileName: " + classFileName + " - Number of Tweets in 2013: " + (totSize-tweets2014Num) + "/" + totSize + " Number of Tweets in 2014: "  + tweets2014Num + "/" + totSize);
                System.out.println("FileName: " + classFileName + " - TrainFileLine: " + trainFileSize + " - TrainValFileLine: " + trainValFileSize + " - TestFileLine: " + testFileSize);
                System.out.println("FileName: " + classFileName + " - TrainFileLine: " + (double)trainFileSize/(totSize) + " - TrainValFileLine: " + (double)trainValFileSize/totSize + " - TestFileLine: " + (double)testFileSize/totSize);

                //build test/train hashtag lists
                fileReaderA = new FileReader(path + classname + "/" + allHashtagList + "_" + classname + ".csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                fw = new FileWriter(path + classname + "/fold" + i + "/" + trainHashtagList + "_t.csv");
                bw = new BufferedWriter(fw);
                fwVal = new FileWriter(path + classname + "/fold" + i + "/" + trainHashtagList + "_v.csv");
                bwVal = new BufferedWriter(fwVal);
                fwAllTrain = new FileWriter(path + classname + "/fold" + i + "/" + trainHashtagList + ".csv");
                bwAllTrain = new BufferedWriter(fwAllTrain);
                fwTest = new FileWriter(path + classname + "/fold" + i + "/" + testHashtagList + ".csv");
                bwTest = new BufferedWriter(fwTest);

                trainFileSize = 0; testFileSize = 0; trainValFileSize = 0;
                while ((line = bufferedReaderA.readLine()) != null) {
                    if (hashtagSetDate.get(line) != null && hashtagSetDate.get(line) <= splitTimestamps[i]) {
                        if(hashtagSetDate.get(line) >= Long.valueOf(splitDatesStr[0])){
                            bwVal.write(line + "\n");
                            trainValFileSize++;
                        }else {
                            trainFileSize++;
                            bw.write(line + "\n");
                        }
                        bwAllTrain.write(line + "\n");
                    }else{
                        testFileSize++;
                        bwTest.write(line + "\n");
                    }
                }
                bw.close();
                bwTest.close();
                bwVal.close();
                bwAllTrain.close();
                bufferedReaderA.close();
                totSize = trainFileSize + trainValFileSize+testFileSize;
                System.out.println("FileName: " + classFileName + " - TrainHashtagLine: " + trainFileSize + " - TrainValHashtagLine: " + trainValFileSize + " - TestHashtagLine: " + testFileSize);
                System.out.println("FileName: " + classFileName + " - TrainHashtagLine: " + (double)trainFileSize/(totSize) + " - TrainValHashtagLine: " + (double)trainValFileSize/(totSize) + " - TestHashtagLine: " + (double)testFileSize/totSize);
                fw = new FileWriter(path + classname + "/fold" + i + "/" + splitTimestamps[i] + ".timestamp");
                bw = new BufferedWriter(fw);
                bw.write(splitTimestamps[i] + "\n");
                bw.write(splitDatesStr[0] + "\n");
                bw.close();
            }
        }
        for(int i = 0; i < numOfFolds; i++)
            System.out.println(splitTimestamps[i]);
    }

    private static String[] findSplitDates(Map<String, Long> hashtagSetDate, int groupNum) {
        long date50, date60;
        String[] dates;
        List<String[]> splitDates = new ArrayList<>(numOfTopics);
        int length, length50, length60, length100;
        List<Long> hashtagSet = new ArrayList<>();

        hashtagSet = new ArrayList<>();
        dates = new String[2];
        hashtagSet.addAll(hashtagSetDate.values());
        Collections.sort(hashtagSet);
        length = hashtagSet.size();
        length50 = (int) Math.ceil((double)length*percentageTrain);
        if(Objects.equals(hashtagSet.get(length50), hashtagSet.get(length50 + 1)))
            System.out.println("Equal");
        length60 = (int)Math.ceil((double)length*percentageVal);
        if(Objects.equals(hashtagSet.get(length60), hashtagSet.get(length60 + 1)))
            System.out.println("Equal");
        date50 = hashtagSet.get(length50);
        date60 = hashtagSet.get(length60);

        dates[0] = String.valueOf(date50);
        dates[1] = String.valueOf(date60);
        return dates;
        //splitDates.add(dates);
        //return splitDates;
    }

    public static void findTestTrain(int groupNum) throws IOException, ParseException {
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        fileReaderA = new FileReader(path + featurepath + hashtagFileName + "_"+groupNum + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        hashtagMap = new HashMap<>();
        String line;
        while ((line = bufferedReaderA.readLine()) != null) {
            if(line.split(",").length > 2)
                hashtagMap.put(line.split(",")[1], Long.valueOf(line.split(",")[2]));
            else
                hashtagMap.put(line.split(",")[0], Long.valueOf(line.split(",")[1]));
        }
        bufferedReaderA.close();
        /*fileReaderA = new FileReader(path + indexFileName);
        bufferedReaderA = new BufferedReader(fileReaderA);
        indexMap = new HashMap<>();
        while ((line = bufferedReaderA.readLine()) != null) {
            indexMap.put(line.split(",")[0], Double.valueOf(line.split(",")[1]));
        }*/
    }


    public static void findTopicalTest(String fileName, String hashtagListName, String classname, int classInd) throws IOException, InterruptedException {
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        String line;
        //Set<Long> testHashtagIndexes;
        Set<String> testHashtagTexts;
        String[] splits;
        FileWriter fwTest;
        BufferedWriter bwTest;
        boolean topical = false;
        int counter = 0;
        String line2 = "";
        boolean flag = false;
        //String classname = "socialissues";{
        counter = 0;
        String textLine = "";
        for (int i = 0; i < numOfFolds; i++) {
            if(fileName.equals("testTrain_train__t")) {
                total[classInd][i] = 0;
                positives[classInd][i] = 0;
            }else if(fileName.equals("testTrain_train__v")){
                totalVal[classInd][i] = 0;
                positivesVal[classInd][i] = 0;
            }
            fileReaderA = new FileReader(path + classname + "/fold" + i + "/" + hashtagListName +".csv");
            bufferedReaderA = new BufferedReader(fileReaderA);
            //testHashtagIndexes = new HashSet<>();
            testHashtagTexts = new HashSet<>();
            while ((line = bufferedReaderA.readLine()) != null) {
                testHashtagTexts.add(line);
                //testHashtagIndexes.add(hashtagMap.get(line));
                //System.out.println(hashtagMap.get(line));
            }
            bufferedReaderA.close();
            System.out.println("========================ClassName - foldNum: " + classname +"-"+ i + "-" + fileName +  "============================");
            fileReaderA = new FileReader(path + classname + "/fold" + i + "/" +  fileName  + ".csv");
            bufferedReaderA = new BufferedReader(fileReaderA);
            fwTest = new FileWriter(path + classname + "/fold" + i + "/" +  fileName  + "_edited.csv");
            bwTest = new BufferedWriter(fwTest);
            while ((line = bufferedReaderA.readLine()) != null) {
                textLine = bufferedReaderA.readLine();
                if(fileName.equals("testTrain_train__t"))
                    total[classInd][i]++;
                else if(fileName.equals("testTrain_train__v"))
                    totalVal[classInd][i]++;

                topical = false;
                if(line.length() == 1) {
                    bwTest.write(line + "\n");
                    continue;
                }
                if(line.substring(0,1).equals("1")){
                    flag = true;
                    line2 = line;
                }
                line2 = line;
                line = line.substring(2, line.length());
                textLine = textLine.substring(2, textLine.length());
                splits = textLine.split(" ");
                //splits[splits.length-1] = splits[splits.length-1].split(":")[0];
                for(int k = 0; k < splits.length; k++) {
                    if(splits[k].split(":").length < 2)
                        continue;
                    //if (testHashtagIndexes.contains(Long.valueOf(splits[k]))) {
                    if (testHashtagTexts.contains(splits[k].split(":")[1])) {
                        topical = true;
                        if(hashtagListName.equals("testTrain_train__t") && total[classInd][i] == 1)
                            System.out.println("Error: First label is 1");
                        break;
                    }
                }
                //if(flag != topical) System.out.println(line2);
                flag = false;
                if(topical) {
                    counter++;
                    bwTest.write("1 ");
                    if(fileName.equals("testTrain_train__t"))
                        positives[classInd][i]++;
                    else if(fileName.equals("testTrain_train__v"))
                        positivesVal[classInd][i]++;
                }else
                    bwTest.write("0 ");
                bwTest.write(line + "\n");
            }
            System.out.println(counter);
            bufferedReaderA.close();
            bwTest.close();
            tweetUtil.runStringCommand("rm -f " + path + classname + "/fold" + i + "/" + fileName + ".csv");
            tweetUtil.runStringCommand("mv " + path + classname + "/fold" + i + "/" + fileName + "_edited.csv " + path + classname + "/fold" + i + "/" + fileName + ".csv");
        }
    }

    public static void modifyFeatureList() throws IOException, InterruptedException {
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        String line;
        FileWriter fwTest, fw;
        BufferedWriter bwTest, bw;
        fileReaderA = new FileReader(path + featurepath + indexFileName);
        bufferedReaderA = new BufferedReader(fileReaderA);
        fwTest = new FileWriter(path + featurepath + indexFileName + "_edited.csv");
        bwTest = new BufferedWriter(fwTest);
        fw = new FileWriter(path + featurepath + hashtagFileName);
        bw = new BufferedWriter(fw);
        int ind = 1;
        if(testFlag){
            while ((line = bufferedReaderA.readLine()) != null) {
                line = line.toLowerCase();
                if (line.contains("hashtag")){
                    bwTest.write("hashtag," + line + "\n");
                    bw.write(line + "\n");
                }else if (line.contains("mentionuser"))
                    bwTest.write("mention," + line + "\n");
                else if (line.contains("user"))
                    bwTest.write("from," + line + "\n");
                else if (line.contains("term"))
                    bwTest.write("term," + line + "\n");
                else if (line.contains("loc"))
                    bwTest.write("location," + line + "\n");
                ind++;
            }
        }else {
            while ((line = bufferedReaderA.readLine()) != null) {
                line = line.toLowerCase();
                if (ind <= 361789)
                    bwTest.write("from," + line + "\n");
                else if (ind <= 676753)
                    bwTest.write("term," + line + "\n");
                else if (ind <= 864339) {
                    bwTest.write("hashtag," + line + "\n");
                    bw.write(line + "\n");
                } else if (ind <= 864339 + 244478)
                    bwTest.write("mention," + line + "\n");
                else
                    bwTest.write("location," + line + "\n");
                ind++;
            }
        }
        bw.close();
        bwTest.close();
        bufferedReaderA.close();
        tweetUtil.runStringCommand("rm -f " + path + featurepath + indexFileName);
        tweetUtil.runStringCommand("mv " + path + featurepath + indexFileName + "_edited.csv " + path + featurepath + indexFileName);
    }
}
