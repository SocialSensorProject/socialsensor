package functionalGradient;

import machinelearning.LearningProblem;
import util.ConfigRead;
import util.TweetUtil;

import java.io.*;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by zahraiman on 2/1/16.
 */
public class TweetToArff {

    public static int k;
    public static TweetUtil tweetUtil;

    public TweetToArff(int _numOfFeatures) throws IOException {
        tweetUtil = new TweetUtil();
        k = _numOfFeatures;
    }

    public void writeHeader(BufferedWriter bw, LearningProblem learningProblem) throws IOException {
        ConfigRead configRead = new ConfigRead();
        BufferedReader bufferedReaderA;
//      bufferedReaderA = new BufferedReader(new FileReader(configRead.get1mFeaturePath()));


        bw.write("@RELATION Name1\n");
        bw.write("@ATTRIBUTE topical integer\n");
        String[]  splits = null;
        String s;
//        long v = -1;
        //while ((line = bufferedReaderA.readLine()) != null) {
        for(long v : learningProblem.featureMap.values()){
//            splits = line.split(",");
//            s = splits[0]+":"+splits[1];
//            if(!learningProblem.featureMap.containsKey(s.toLowerCase()))
//                continue;
//            v = learningProblem.featureMap.get(s.toLowerCase());
            bw.write("@ATTRIBUTE " + v +" {0,1}\n");
        }
        bw.write("\n@data\n");
        //bw.close();
    }

    public List<HashSet<String>> makeArffTestTrainSplits(LearningProblem learningProblem, int classInd) throws ParseException, IOException, InterruptedException {
        //TODO: make sure about classname
        double y;
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        FileWriter fw, fwTest, fwVal, fwAllTrain, fwStrings, fwValStrings, fwAllTrainStrings, fwTestStrings;
        BufferedWriter bw, bwTest, bwVal, bwAllTrain, bwStrings, bwValStrings, bwAllTrainStrings, bwTestStrings;
        int[] counts = new int[4];
        ArrayList<Integer> tmp;
        String cleanLine = "", lineName, textLine, cleanTextLine, line;
        String[] splits, features, splitSt;
        HashSet<String> set, trainHashtags, trainTrainHashtags;
        String classname = LearningProblem.classNames[classInd-1];
        String trainFileName = LearningProblem.trainFileName;
        String testFileName = LearningProblem.testFileName;
        int trainFileSize = 0, testFileSize = 0, trainValFileSize = 0;
        long cDate, tweets2014Num = 0;
        int numOfHashtags;
        String[] splitDatesStr = learningProblem.splitDatesStr[classInd-1];
        int totalVal = 0, total = 0, positivesVal = 0, positives = 0;

        trainHashtags = new HashSet<>();
        HashSet<String> testHashtags = new HashSet<>();
        trainTrainHashtags = new HashSet<>();
        HashSet<String> trainValHashtags = new HashSet<>();
        String[] hNames = {LearningProblem.trainHashtagList, LearningProblem.testHashtagList, LearningProblem.trainHashtagList + "_t", LearningProblem.trainHashtagList + "_v"};
        boolean topicalVal, topicalTrain, topicalTraintrain, topicalTest;
        for(String hName : hNames) {
            fileReaderA = new FileReader(LearningProblem.path + LearningProblem.classNames[classInd-1] + "/fold" + k + "/" + hName + ".csv");
            bufferedReaderA = new BufferedReader(fileReaderA);
            while ((line = bufferedReaderA.readLine()) != null) {
                switch(hName) {
                    case("trainHashtagList"):
                        trainHashtags.add("hashtag:" + line) ;
                        break;
                    case("testHashtagList"):
                        testHashtags.add("hashtag:" + line);
                        break;
                    case("trainHashtagList_t"):
                        trainTrainHashtags.add("hashtag:" + line) ;
                        break;
                    case("trainHashtagList_v"):
                        trainValHashtags.add("hashtag:" + line);
                        break;
                }
            }
            bufferedReaderA.close();
        }

//        tweetUtil.runStringCommand("perl -MList::Util -e 'print List::Util::shuffle <>' " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet.csv" + " > " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet2.csv");
//        tweetUtil.runStringCommand("rm -f " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet.csv");
//        tweetUtil.runStringCommand("mv " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet2.csv" + " " + path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd+1)+"_allInnerJoins_parquet.csv");
        fileReaderA = new FileReader(LearningProblem.path + "out_tweet_hashtag_user_mention_term_time_location_"+(classInd)+"_allInnerJoins_parquet.csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        fw = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + trainFileName+ "_t.arff");
        bw = new BufferedWriter(fw);
        fwTest = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" +  testFileName  + ".arff");
        bwTest = new BufferedWriter(fwTest);
        fwVal = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + trainFileName  + "_v.arff");
        bwVal = new BufferedWriter(fwVal);
        fwAllTrain = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" +  trainFileName + ".arff");
        bwAllTrain = new BufferedWriter(fwAllTrain);
        fwStrings = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + trainFileName + "_t_strings.csv");
        bwStrings = new BufferedWriter(fwStrings);
        fwValStrings = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + trainFileName  + "_v_strings.csv");
        bwValStrings = new BufferedWriter(fwValStrings);
        fwAllTrainStrings = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + trainFileName + "_strings.csv");
        bwAllTrainStrings = new BufferedWriter(fwAllTrainStrings);
        fwTestStrings = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + testFileName + "_strings.csv");
        bwTestStrings = new BufferedWriter(fwTestStrings);

        //WRITE THE HASHTAG LIST BASED ON TIMESTAMP

        writeHeader(bw, learningProblem);
        writeHeader(bwTest, learningProblem);
        writeHeader(bwVal, learningProblem);
        writeHeader(bwAllTrain, learningProblem);
        int sampleInd = 0;
        double yhatOne , yhatNegOne;
        while ((textLine = bufferedReaderA.readLine()) != null) {
            y = 0;
            numOfHashtags = 0;
            cleanTextLine= "";
            textLine = textLine.substring(2, textLine.length());
            splits = textLine.split(" ");
            long tid = Long.valueOf(splits[splits.length-1]);
            features = new String[splits.length-2];
            int index = 0;
            String[] strs;
            topicalVal = false; topicalTrain = false; topicalTraintrain = false; topicalTest = false;
            for (String split : splits) {
                split = split.toLowerCase();
                strs = split.split(":");
                if (strs.length < 2)
                    continue;
                if (trainValHashtags.contains(split))
                    topicalVal = true;
                if (trainHashtags.contains(split))
                    topicalTrain = true;
                if (trainTrainHashtags.contains(split))
                    topicalTraintrain = true;
                if (testHashtags.contains(split))
                    topicalTest = true;
                if(strs[0].equals("hashtag"))
                    numOfHashtags++;
                features[index] = split;
                index++;
            }
            if(features.length == 0)
                continue;
            cDate = Long.valueOf(splits[splits.length-2]);

            set = new HashSet<String>(features.length);
            Collections.addAll(set, features);
            tmp = new ArrayList<>();
            for (String s : set) {
                if(learningProblem.featureMap == null || s == null)
                    continue;;
                if(learningProblem.featureMap.get(s.toLowerCase()) == null)
                    continue;
                cleanTextLine += "," + s + " 1";
                tmp.add(learningProblem.featureMap.get(s.toLowerCase()));
            }
            //cleanTextLine += " "  + cDate + " " + tid;
            cleanTextLine += " "  + tid;
            if(tmp.size() == 0)
                continue;
            Collections.sort(tmp);
            cleanLine = "";
            for (long st : tmp)
                cleanLine += "," + new BigDecimal(st).toPlainString() + " 1";

            if(cDate > 1388534339000l)
                tweets2014Num++;

            yhatOne = 1;//computeYHat(1, fFun, iteartion, trainFileSize);
            yhatNegOne = -1;//computeYHat(-1, fFun, iteartion, trainFileSize);

            if (cDate <= Long.valueOf(learningProblem.getSplitDatesStr()[classInd-1][1])) {
                if(cDate >= Long.valueOf(splitDatesStr[0])){//TRAIN_VAL
                    totalVal++;
                    if(topicalVal) {
                        positivesVal++;
                        bwVal.write("{0 "+ yhatOne + cleanLine + "}\n");
                        bwValStrings.write(yhatOne + ((topicalTraintrain)? " 1" : " 0") + ((numOfHashtags == 0)? " 0" : " 1") +cleanTextLine + "\n");
                    }else {
                        bwVal.write("{0 "+ yhatNegOne + cleanLine + "}\n");
                        bwValStrings.write(yhatNegOne + ((topicalTraintrain)? " 1" : " 0") + ((numOfHashtags == 0)? " 0" : " 1") + cleanTextLine + "\n");
                    }
                    trainValFileSize++;
                }else {//TRAIN_TRAIN
                    //yhatOne = computeYHat(1, fFun, iteartion, trainFileSize);
                    //yhatNegOne = computeYHat(-1, fFun, iteartion, trainFileSize);
                    total++;
                    if(topicalTraintrain) {
                        if(total < 2) {
                            System.out.println(" ERROR : First label is 1");
                            //firstClassOne = true;
                        }
                        positives++;
                        bw.write("{0 "+ yhatOne + cleanLine + "}\n");
                        bwStrings.write(yhatOne + cleanTextLine + "\n");
                    }else {
                        bw.write("{0 "+yhatNegOne + cleanLine + "}\n");
                        bwStrings.write(yhatNegOne + cleanTextLine + "\n");
                    }
                    trainFileSize++;
                }
                if(topicalTrain) {
                    bwAllTrain.write("{0 "+ yhatOne + cleanLine + "}\n");
                    bwAllTrainStrings.write(yhatOne + cleanTextLine + "\n");
                }else {
                    bwAllTrain.write("{0 "+yhatNegOne + cleanLine + "}\n");
                    bwAllTrainStrings.write(yhatNegOne + cleanTextLine + "\n");
                }
            }
            else {
                if(topicalTest) {
                    bwTest.write("{0 "+yhatOne + cleanLine + "}\n");
                    bwTestStrings.write(yhatOne + ((topicalTrain)? " 1" : " 0") + ((numOfHashtags == 0)? " 0" : " 1") + cleanTextLine + "\n");
                }else {
                    bwTest.write("{0 "+yhatNegOne + cleanLine + "}\n");
                    bwTestStrings.write(yhatNegOne + ((topicalTrain)? " 1" : " 0") + ((numOfHashtags == 0)? " 0" : " 1") + cleanTextLine + "\n");
                }
                testFileSize++;
            }
            sampleInd++;
        }
        bufferedReaderA.close();
        bw.close();
        bwTest.close();
        bwVal.close();
        bwAllTrain.close();
        bwStrings.close();
        bwTestStrings.close();
        bwValStrings.close();
        bwAllTrainStrings.close();
        int totSize = trainFileSize+trainValFileSize+testFileSize;
        System.out.println("FileName: " + classname + " - Number of Tweets in 2013: " + (totSize - tweets2014Num) + "/" + totSize + " Number of Tweets in 2014: " + tweets2014Num + "/" + totSize);
        System.out.println("FileName: " + classname + " - TrainFileLine: " + trainFileSize + " - TrainValFileLine: " + trainValFileSize + " - TestFileLine: " + testFileSize);
        System.out.println("FileName: " + classname + " - TrainFileLine: " + (double) trainFileSize / (totSize) + " - TrainValFileLine: " + (double) trainValFileSize / totSize + " - TestFileLine: " + (double) testFileSize / totSize);

        //build test/train hashtag lists

        bw.close();
        bwTest.close();
        bwVal.close();
        bwAllTrain.close();
        bufferedReaderA.close();
        fw = new FileWriter(LearningProblem.path + classname + "/fold" + k + "/" + splitDatesStr[1] + ".timestamp");
        bw = new BufferedWriter(fw);
        bw.write(splitDatesStr[1] + "\n");
        bw.write(splitDatesStr[0] + "\n");
        bw.close();


        List<HashSet<String>> allHashtags = new ArrayList<>();
        allHashtags.add(trainHashtags);
        allHashtags.add(trainTrainHashtags);
        allHashtags.add(trainValHashtags);
        allHashtags.add(testHashtags);

        learningProblem.setTotalVal(totalVal, classInd-1);
        learningProblem.setTotal(total, classInd-1);
        learningProblem.setPositivesVal(positivesVal, classInd - 1);
        learningProblem.setPositives(positives, classInd-1);
        learningProblem.setTrainFileSize(trainFileSize, classInd-1);
        learningProblem.setTrainValFileSize(trainValFileSize, classInd-1);
        learningProblem.setTestFileSize(testFileSize, classInd-1);

        return allHashtags;
    }

    public void makeHashtagSets(LearningProblem learningProblem, int classInd) throws IOException, ParseException {
        ConfigRead configRead = new ConfigRead();
        FileReader fileReaderA;
        String classname = configRead.getGroupNames()[classInd-1];
        BufferedReader bufferedReaderA;
        FileWriter fw, fwTest, fwVal, fwAllTrain;
        BufferedWriter bw, bwTest, bwVal, bwAllTrain;

        ArrayList<Long> tmp;
        String line;
        String[] splitSt;
        String[] splitDatesStr;
        long[] splitTimestamps = new long[2];

        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        HashMap<String, Long> hashtagDate = new HashMap<>();

        fileReaderA = new FileReader(LearningProblem.path +learningProblem.featurepath + learningProblem.hashtagSetDateName);
        bufferedReaderA = new BufferedReader(fileReaderA);
        while ((line = bufferedReaderA.readLine()) != null) {
            splitSt = line.split(",");
            hashtagDate.put(splitSt[0].toLowerCase(), Long.valueOf(splitSt[1]));
        }
        bufferedReaderA.close();

        Map<String, Long> hashtagSetDate = new HashMap<>();
        for (String s : tweetUtil.getGroupHashtagList(classInd, configRead.getTestFlag(), learningProblem.path + classname + "/" + learningProblem.allHashtagList + "_" + classname + ".csv")) {
            if (hashtagDate.containsKey(s))// && featureMap.containsKey(s)) {
                hashtagSetDate.put(s, hashtagDate.get(s));
        }

        //build test/train data and hashtag lists
        int firstLabel;

        int tweets2014Num = 0;long cDate;
        learningProblem.featureMap = new HashMap<>();
        fileReaderA = new FileReader(learningProblem.path +learningProblem.featurepath + learningProblem.indexFileName + "_" + classname+"_" + k + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        while ((line = bufferedReaderA.readLine()) != null) {
            splitSt = line.split(",");
            learningProblem.addFeatureOrders(Integer.valueOf(splitSt[2]));
            learningProblem.featureMap.put(splitSt[0]+":"+splitSt[1].toLowerCase(), Integer.valueOf(splitSt[2]));
        }
        bufferedReaderA.close();
        if (!configRead.getTestFlag())
            splitDatesStr = tweetUtil.findSplitDates(hashtagSetDate, learningProblem.percentageTrain, learningProblem.percentageVal);
        else {
            splitDatesStr = new String[]{String.valueOf(format.parse("Wed Nov 20 14:08:01 +0001 2013").getTime()), String.valueOf(format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime())};
        }
        splitTimestamps[0] = Long.valueOf(splitDatesStr[1]);

        fileReaderA = new FileReader(learningProblem.path + classname + "/" + learningProblem.allHashtagList + "_" + classname + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        fw = new FileWriter(learningProblem.path + classname + "/fold" + k + "/" + learningProblem.trainHashtagList + "_t.csv");
        bw = new BufferedWriter(fw);
        fwVal = new FileWriter(learningProblem.path + classname + "/fold" + k + "/" + learningProblem.trainHashtagList + "_v.csv");
        bwVal = new BufferedWriter(fwVal);
        fwAllTrain = new FileWriter(learningProblem.path + classname + "/fold" + k + "/" + learningProblem.trainHashtagList + ".csv");
        bwAllTrain = new BufferedWriter(fwAllTrain);
        fwTest = new FileWriter(learningProblem.path + classname + "/fold" + k + "/" + learningProblem.testHashtagList + ".csv");
        bwTest = new BufferedWriter(fwTest);

        int trainFileSize = 0, testFileSize = 0, trainValFileSize = 0;
        while ((line = bufferedReaderA.readLine()) != null) {
            if (hashtagSetDate.get(line) != null && hashtagSetDate.get(line) <= Long.valueOf(splitDatesStr[1])) {
                if(hashtagSetDate.get(line) >= Long.valueOf(splitDatesStr[0])){
                    bwVal.write(line + "\n");
                    trainValFileSize++;
                }else {
                    trainFileSize++;
                    bw.write(line + "\n");
                }
                bwAllTrain.write(line + "\n");
            } else{
                testFileSize++;
                bwTest.write(line + "\n");
            }
        }
        bw.close();
        bwAllTrain.close();
        bwTest.close();
        bwVal.close();

        learningProblem.setSplitDatesStr(splitDatesStr, classInd-1);
    }

    public double computeYHat(int y, double[][] fFun, int iteration, int ind){
        double yhat = -2;
        yhat = (2*y) / (1 + Math.exp(-2 * y * fFun[0][ind])); // TODO: use fFun[0]
        return yhat;
    }
}
