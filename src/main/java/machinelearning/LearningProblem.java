package machinelearning;

import ddInference.src.logic.add.FBR;
import util.ConfigRead;
import util.TweetResult;
import util.TweetUtil;

import java.io.*;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by zahraiman on 2/3/16.
 */
public class LearningProblem {

    public static Map<String, Long> hashtagMap;
    public static Map<String, Long> indexMap;
    public static DecimalFormat df3 = new DecimalFormat("#.###");
    public static int featureNum = 1000000;
    public static int sampleNum = 2000000;
    public static TweetUtil tweetUtil;

    public static String path;// = "/data/ClusterData/input/Data/Learning/Topics/";
    public static String trecPath;// = "/data/OSU_DocAnalysis_Fall2015_Assign1-master/trec_eval.8.1";
    public static String NBPath;// = "/data/ClusterData/input/Data/LearningMethods/";
    public static String LRPath;// = "/data/ClusterData/input/Data/Learning/LogisticRegression/";
    public static String rankSVMPath;// = "/data/liblinear-ranksvm-1.95/train";
    public static String featurepath = "featureData/";
    public static String hashtagFileName = "hashtagIndex";
    public static String indexFileName = "featureIndex";
    public static String allHashtagList = "allHashtag";
    public static String hashtagSetDateName = "hashtagSet_Date.csv";
    public static String testHashtagList = "testHashtagList";
    public static String trainHashtagList = "trainHashtagList";
    public static String trainFileName = "testTrain_train_";
    public static String testFileName = "testTrain_test_";
    public static String outputFileName = "output_disaster";
    public static String modelFileName = "model_disaster";
    public static String solverType;
    public static int numOfFolds = 1;
    public static int numOfTopics;
    public static String[] classNames;

    public static ConfigRead configRead;
    public static boolean testFlag;
    public static Map<Integer, String> invFeatures;
    public static double percentageTrain = 0.4;
    public static double percentageVal = 0.6;
    public static Map<String, Long> hashtagDate;
    public static HashSet<String> trainHashtags;
    public static HashSet<String> trainTrainHashtags;
    public static boolean firstClassOne;
    public static int topTweetsNum = 20;
    public static int totalFeatureNum =1166582;

    public Map<String, Integer> featureMap;
    public HashMap<Integer, String> inverseFeatureMap;
    private static ArrayList<Integer> featureOrders;
    private static int[] trainFileSize;
    private static int[] testFileSize;
    private static int[] trainValFileSize;
    private static int[] total;
    private static int[] totalVal;
    private static int[] positives;
    private static int[] positivesVal;
    public static String[][] splitDatesStr;

    public int[] getTestFileSize() {
        return testFileSize;
    }

    public int[] getTrainFileSize() {
        return trainFileSize;
    }

    public void setTrainFileSize(int _trainFileSize, int classInd) {
        if(trainFileSize == null)
            trainFileSize = new int[configRead.getNumOfGroups()];
        trainFileSize[classInd] = _trainFileSize;
    }

    public ArrayList<Integer> getFeatureOrders() {
        return featureOrders;
    }

    public void addFeatureOrders(int _featureOrder) {
        if(featureOrders == null)
            featureOrders = new ArrayList<>();
        featureOrders.add(_featureOrder);
    }

    public void setTestFileSize(int _testFileSize, int classInd) {
        if(testFileSize == null)
            testFileSize = new int[configRead.getNumOfGroups()];
        testFileSize[classInd] = _testFileSize;
    }

    public int[] getTrainValFileSize() {
        return trainValFileSize;
    }

    public void setTrainValFileSize(int _trainValFileSize, int classInd) {
        if(trainValFileSize == null)
            trainValFileSize = new int[configRead.getNumOfGroups()];
        trainValFileSize[classInd] = _trainValFileSize;
    }

    public int[] getTotal() {
        return total;
    }

    public void setTotal(int _total, int classInd) {
        if(total == null)
            total = new int[configRead.getNumOfGroups()];
        total[classInd] = _total;
    }

    public int[] getTotalVal() {
        return totalVal;
    }

    public void setTotalVal(int _totalVal, int classInd) {
        if(totalVal == null)
            totalVal = new int[configRead.getNumOfGroups()];
        totalVal[classInd] = _totalVal;
    }

    public int[] getPositives() {
        return positives;
    }

    public void setPositives(int _positives, int classInd) {
        if(positives == null)
            positives = new int[configRead.getNumOfGroups()];
        positives[classInd] = _positives;
    }

    public int[] getPositivesVal() {
        return positivesVal;
    }

    public void setPositivesVal(int _positivesVal, int classInd) {
        if(positivesVal == null)
            positivesVal = new int[configRead.getNumOfGroups()];
        positivesVal[classInd] = _positivesVal;
    }

    public String[][] getSplitDatesStr() {
        return splitDatesStr;
    }

    public void setSplitDatesStr(String[] _splitDatesStr, int classInd) {
        if(splitDatesStr == null)
            splitDatesStr = new String[configRead.getNumOfGroups()][2];
        splitDatesStr[classInd] = _splitDatesStr;
    }

    public LearningProblem() throws IOException {
        configRead = new ConfigRead();
        path = configRead.getLearningPath();
        LRPath = configRead.getLRPath();
        NBPath = configRead.getNBPath();
        trecPath = configRead.getTrecPath();
        rankSVMPath = configRead.getRankSVMPath();
        testFlag = configRead.getTestFlag();
        System.out.println("TEST FLAG: " + testFlag);
        if(configRead.getTrainPercentage() == 0.7){
            percentageTrain = 0.7;
            percentageVal = 0.8;
        }
        tweetUtil = new TweetUtil();
        if(testFlag){
            path = "Data/test/Learning/Topics/";
            NBPath = "Data/test/LearningMethods/";
            LRPath = "Data/test/Learning/LogisticRegression/";
            classNames = new String[] {"naturaldisaster"};
            numOfTopics = 1;
        }else{
            numOfTopics = configRead.getNumOfGroups();
            classNames = configRead.getGroupNames();
        }
        positives = new int[classNames.length];
        total = new int[classNames.length];
        positivesVal = new int[classNames.length];
        totalVal = new int[classNames.length];
    }

    public void getFeatureList(double k, String classname) throws IOException, InterruptedException {
        FileReader fileReaderA = new FileReader(path + classname + "/featuresMI.csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        FileWriter fw = new FileWriter(path +featurepath + indexFileName + "_" + classname+"_" + (int)k + ".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        String line = "";
        int ind = 0;
        String[] splits;
        line = bufferedReaderA.readLine();
        while (line != null && ind < k) {
            splits = line.split(",");
            ind++;
            bw.write(splits[0] + "," + splits[1] + "," + ind + "\n");
            line = bufferedReaderA.readLine();
        }
        bw.close();
        bufferedReaderA.close();
    }

    public ArrayList<String> getSortedMIFeatures(String classname) throws IOException, InterruptedException {
        FileReader fileReaderA = new FileReader(path + classname + "/featuresMI.csv");
        ArrayList<String> topFeaturesNames = new ArrayList<>();
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line = "";
        int ind = 0;
        String[] splits;

        while((line = bufferedReaderA.readLine()) != null){
            splits = line.split(",");
            ind++;
            topFeaturesNames.add(splits[0]+":"+splits[1]);
        }
        bufferedReaderA.close();
        return topFeaturesNames;
    }

    public static double[] computePrecisionMAP(List<TweetResult> tweetWeights, String classname, int classInd, int numOfFeatures, int iteration, String _solverType) throws IOException, InterruptedException {
        double map, p100;
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(LRPath + classname + "/" + "/fold" + numOfFeatures + "/" + "out_" + outputFileName + "_" + iteration + "_qrel" + "_csv"));
        BufferedWriter bufferedWriter2 = new BufferedWriter(new FileWriter(LRPath + classname + "/" + "/fold" + numOfFeatures + "/" + "out_" + outputFileName + "_" + iteration + "_qtop" + "_csv"));
        int index = 0;
        for (int ij = 0; ij < Math.min(10000, tweetWeights.size()); ij++) {
//                            bufferedWriter.write(tr.getWeight() + "," + tr.getText() + "\n");
            bufferedWriter.write(classInd + " " + "Q0" + " " + tweetWeights.get(ij).getTid() + " " + tweetWeights.get(ij).getTopical() + "\n");
            bufferedWriter2.write(classInd + " " + "Q0" + " " + tweetWeights.get(ij).getTid() + " " + index + " " + new BigDecimal(tweetWeights.get(ij).getWeight()).toPlainString() + " out_" + "\n");
            index++;
        }
        bufferedWriter.close();
        bufferedWriter2.close();

        BufferedReader bufferedReaderA;
        TweetUtil.runStringCommand(trecPath + "/trec_eval -a " + LRPath + classname + "/" +
                "fold" + numOfFeatures + "/" + "out_" + outputFileName + "_" + iteration + "_qrel" +
                "_csv " + LRPath + classname + "/" + "fold" + numOfFeatures + "/" + "out_" +
                outputFileName + "_" + iteration + "_qtop" + "_csv > " + LRPath + classname +
                "/" + "fold" + numOfFeatures + "/" + "out_noTrain_" + outputFileName + "_" + iteration + "_" + _solverType + ".csv");
        TweetUtil.runStringCommand("rm -f " + LRPath + classname + "/" +
                "fold" + numOfFeatures + "/" + "out_" + outputFileName + "_" + iteration + "_qrel" + "_csv ");
        TweetUtil.runStringCommand("rm -f "+LRPath + classname + "/" + "fold" + numOfFeatures + "/" + "out_" +
                        outputFileName + "_" + iteration + "_qtop" + "_csv");

        bufferedReaderA = new BufferedReader(new FileReader(LRPath + classname + "/" + "fold" + numOfFeatures + "/" + "out_noTrain_" + outputFileName + "_" + iteration + "_" + _solverType + ".csv"));

        for (int kk = 0; kk < 4; kk++)//59//4//62
            bufferedReaderA.readLine();
        map = Double.valueOf(bufferedReaderA.readLine().split("map            \tall\t")[1]);
        for (int kk = 0; kk < 54; kk++)
            bufferedReaderA.readLine();
        p100 = Double.valueOf(bufferedReaderA.readLine().split("P100           \tall\t")[1]);
        bufferedReaderA.close();
        TweetUtil.runStringCommand("rm -f "+LRPath + classname +
                "/" + "fold" + numOfFeatures + "/" + "out_noTrain_" + outputFileName + "_" + iteration + "_" + _solverType + ".csv");
        return new double[]{map, p100};
    }
    
    public static void prepareDirectories(int[] kValues) throws IOException, InterruptedException {
        for(String classname : classNames) {
            tweetUtil.runStringCommand("mkdir " + path + classname);
            tweetUtil.runStringCommand("mkdir " + LRPath);
            tweetUtil.runStringCommand("mkdir " + LRPath + classname);
            for (int k : kValues) {
                tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + k);
                tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + k + "/l2_lr");
                tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + k + "/l1_lr");
                tweetUtil.runStringCommand("mkdir " + path + classname + "/" + "fold" + k + "/l2_lrd");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/" + "fold" + k);
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr" + "/" + "fold" + k);
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr" + "/" + "fold" + k + "/bestc");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lr" + "/" + "fold" + k + "/bestk");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/rankSVM");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/rankSVM" + "/" + "fold" + k);
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/rankSVM" + "/" + "fold" + k + "/bestc");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/rankSVM" + "/" + "fold" + k + "/bestk");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr" + "/" + "fold" + k);
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr" + "/" + "fold" + k + "/bestc");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l1_lr" + "/" + "fold" + k + "/bestk");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd" + "/" + "fold" + k);
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd" + "/" + "fold" + k + "/bestc");
                tweetUtil.runStringCommand("mkdir " + LRPath + classname + "/l2_lrd" + "/" + "fold" + k + "/bestk");
            }
        }
    }

    public static ArrayList<Feature> writeFeatureFile(String classname, String modelName, int k, String featurePath, String valTest, double lambda) throws IOException, InterruptedException {
        //build test/train data and hashtag lists
        ArrayList<Feature> features = new ArrayList<>();
        FileReader fileReaderA = new FileReader(modelName);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        FileReader fileReaderB = new FileReader(featurePath);
        BufferedReader bufferedReaderB = new BufferedReader(fileReaderB);
        tweetUtil.runStringCommand("mkdir " + path + classname +"/fold" + k +  "/" + valTest + "/");
        tweetUtil.runStringCommand("mkdir " + path + classname +"/fold" + k +  "/" + valTest + "/" + solverType + "/");
        FileWriter fw = new FileWriter(path + classname +"/fold" + k +  "/" + valTest + "/" + solverType + "/featureWeights_" + lambda + ".csv");
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
            bw.write(splits[0].toLowerCase() + ":" + splits[1].toLowerCase() + "," + featureWeights.get(ik) + "\n");
            features.add(new Feature(splits[0].toLowerCase() + ":" + splits[1].toLowerCase(), Double.valueOf(featureWeights.get(ik))));
        }
        fileReaderA.close();
        fileReaderB.close();
        bw.close();
        tweetUtil.runStringCommand("sort -t',' -rn -k3,3 " + path + classname + "/fold" + k + "/" + valTest + "/" + solverType + "/featureWeights_" + lambda + ".csv > " + path + classname + "/fold" + k + "/" + valTest + "/" + solverType + "/featureWeights1_" + lambda + ".csv");
        tweetUtil.runStringCommand("rm -rf " + path + classname + "/fold" + k + "/" +  valTest + "/" + solverType + "/featureWeights_" + lambda + ".csv");
        tweetUtil.runStringCommand("mv " + path + classname + "/fold" + k + "/" +  valTest + "/" + solverType + "/featureWeights1_" + lambda + ".csv " + path + classname + "/fold" + k + "/" +  valTest + "/" + solverType + "/featureWeights_" + lambda + ".csv");
        //return Double.valueOf(featureWeights.get(featureWeights.size()-1));
        return features;
    }

    public void MakeHashtagLists(String className, HashSet<String> trainHashtags, HashSet<String> trainTrainHashtags, HashSet<String> testHashtags, HashSet<String> trainValHashtags, int numOfFeatures) throws IOException {
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        String line;
        String[] hNames = {trainHashtagList, testHashtagList, trainHashtagList + "_t", trainHashtagList + "_v"};
        for(String hName : hNames) {
            fileReaderA = new FileReader(LearningProblem.path + className + "/fold" + numOfFeatures + "/" + hName + ".csv");
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
    }
}
