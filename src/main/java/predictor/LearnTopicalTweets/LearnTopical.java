package predictor.LearnTopicalTweets;

import predictor.de.bwaldvogel.liblinear.InvalidInputDataException;
import predictor.de.bwaldvogel.liblinear.Predict;
import predictor.de.bwaldvogel.liblinear.Train;
import util.Statistics;

import java.io.*;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by imanz on 9/24/15.
 */
public class LearnTopical {
    private static Map<String, Double> hashtagMap;
    private static Map<String, Double> indexMap;
    private static DecimalFormat df3 = new DecimalFormat("#.###");
    private static int featureNum = 1000000;
    private static int sampleNum = 2000000;

    private static String path = "Data/Learning/Topics/";
    private static String LRPath = "Data/Learning/LogisticRegression/";
    private static String featurepath = "featureData/";
    private static String hashtagFileName = "hashtagIndex.csv";
    private static String indexFileName = "featureIndex.csv";
    private static String allHashtagList = "allHashtag";
    private static String hashtagSetDate = "hashtagSet_Date.csv";
    private static String testHashtagList = "testHashtagList";
    private static String trainHashtagList = "trainHashtagList";
    private static String disasterFileName = "tweet_hashtag_user_mention_term_time_1_allInnerJoins.csv";
    private static String politicFileName = "tweet_hashtag_user_mention_term_time_2_allInnerJoins.csv";
    private static String trainFileName = "testTrain_train_";
    private static String testFileName = "testTrain_test_";
    private static String outputFileName = "output_disaster";
    private static String modelFileName = "model_disaster";
    private static String solverType = "L2_LR";
    private static int numOfFolds = 10;
    private static int numOfTopics = 2;

    public static void main1(String[] args) throws IOException, InvalidInputDataException {

        //String solverType = "L1_LR";

        Train train = new Train();

        String[] arguments = new String[50];
        int ind = 0;
        arguments[ind] = "-c";ind++;
        arguments[ind] = "10";ind++;
        arguments[ind] = "-v";ind++;
        arguments[ind] = "10";ind++;
        arguments[ind] = "-s";ind++;
        if(solverType.equals("L2_LR"))
            arguments[ind] = "0";
        else if(solverType.equals("L1_LR"))
            arguments[ind] = "6";
        ind++;
        arguments[ind] = "-B";ind++;
        arguments[ind] = "1";ind++;
        //arguments[ind] = "-w3";ind++;
        //arguments[ind] = "2";ind++;
        arguments[ind] = path+trainFileName;ind++;
        arguments[ind] = path+solverType+"_"+modelFileName;ind++;
        Arrays.copyOfRange(arguments, 0, ind - 1);

        //train.run(arguments);

        Predict predict = new Predict();
        arguments = new String[50];
        ind= 0;
        arguments[ind] = "-b";ind++;
        arguments[ind] = "1";ind++;
        arguments[ind] = path+testFileName;ind++;
        arguments[ind] = path+solverType+"_"+modelFileName;ind++;
        arguments[ind] = path+solverType+"_"+outputFileName;ind++;
        predict.mainPredict(arguments);

    }

    /*
     * Run tests on data
     */
    public static void main(String[] args) throws IOException, InvalidInputDataException, ParseException {
        String time1 = "2013-06-20 15:08:01";
        String time2 = "Thu Jun 20 15:08:01 +0001 2013";
        long t = new SimpleDateFormat("yyy-MM-dd HH':'mm':'ss").parse(time1).getTime();
        long t2 = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse(time2).getTime();
        boolean filePrepare = false;

        if(filePrepare) {
            prepareTestTrainSplits();
            modifyFeatureList();
            findTestTrain();
            findTopicalTest(trainFileName, trainHashtagList);
            findTopicalTest(trainFileName + "_t", trainHashtagList);
            findTopicalTest(trainFileName + "_v", trainHashtagList);
            findTopicalTest(testFileName, testHashtagList);
        }

        ArrayList<Double> accuracies = new ArrayList<Double>();
        ArrayList<Double> precisions = new ArrayList<Double>();
        ArrayList<Double> recalls = new ArrayList<Double>();
        ArrayList<Double> fscores = new ArrayList<Double>();


        //String solverType = "L2_LR";
        String solverType = "L1_LR";

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
        if (solverType.equals("L2_LR"))
            arguments[ind] = "0";
        else if (solverType.equals("L1_LR"))
            arguments[ind] = "6";
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
        double[] cValues = {0.001, 0.01, 0.1, 1, 10, 100};
        double bestC = -1, bestError = sampleNum;
        for (int classNum = 1; classNum <= numOfTopics; classNum++) {
            accuracies = new ArrayList<Double>();
            precisions = new ArrayList<Double>();
            recalls = new ArrayList<Double>();
            fscores = new ArrayList<Double>();

            System.out.println("========================TopicNum: " + classNum + "============================");
            for (int i = 0; i < numOfFolds; i++) {
                bestC = -1;
                bestError = sampleNum;
                System.out.println("========================foldNum: " + i + "============================");
                String trainName =  classNum + "/fold" + i + "/" + trainFileName+ "_t.csv";// + (i+1);
                String testName =  classNum + "/fold" + i + "/" + trainFileName + "_v.csv";//  + (i+1);
                for (double c : cValues) {
                    ind = remInd;
                    predInd = remPredInd;
                    System.out.println("========================C Value: " + c + "============================");
                    arguments[ind] = "-c";ind++;
                    arguments[ind] = String.valueOf(c);ind++;
                    arguments[ind] = path + trainName;ind++;
                    arguments[ind] = LRPath + classNum + "/" + solverType + "/fold" + i + "/" + modelFileName + "_" + c;ind++;
                    Arrays.copyOfRange(arguments, 0, ind - 1);
                    train.run(arguments);

                    argumentsPred[predInd] = path + testName;predInd++;
                    argumentsPred[predInd] = LRPath + classNum + "/" + solverType + "/fold" + i + "/" + modelFileName + "_" + c;predInd++;
                    argumentsPred[predInd] = LRPath + classNum + "/" + solverType + "/fold" + i + "/" + outputFileName + "_" + c;predInd++;
                    Arrays.copyOfRange(argumentsPred, 0, predInd - 1);

                    double[] measures = predict.mainPredict(argumentsPred);
                    //accuracies.add(measures[0]);
                    //precisions.add(measures[1]);
                    //recalls.add(measures[2]);
                    //fscores.add(measures[3]);
                    if (measures[4] < bestError) { //error
                        bestC = c;
                        bestError = measures[4];
                    }
                }
                System.err.println(" For classNum: " + classNum + " and foldNum: " + i + " , the best C is : " + bestC + " with error value of " + bestError);
                //Evaluate on Test with bestC found on train validation data
                testName =  classNum + "/" + solverType + "/fold" + i + "/" +  testFileName + ".csv";//  + (i+1);
                trainName =  classNum + "/" + solverType + "/fold" + i + "/" +  trainFileName + ".csv";//  + (i+1);
                double c = bestC;
                predInd = remPredInd;
                System.out.println("========================Evaluate on Test data with C Value: " + c + "============================");
                ind = remInd;
                arguments[ind] = "-c";ind++;
                arguments[ind] = String.valueOf(c);ind++;
                arguments[ind] = path + trainName;ind++;
                arguments[ind] = LRPath + classNum + "/" + solverType + "/fold" + i + "/bestC/" +  modelFileName + "_" + c;ind++;
                Arrays.copyOfRange(arguments, 0, ind - 1);
                train.run(arguments);
                argumentsPred[predInd] = path + testName;
                predInd++;
                argumentsPred[predInd] = LRPath + classNum + "/" + solverType + "/fold" + i + "/bestC/" +  modelFileName + "_" + c;
                predInd++;
                argumentsPred[predInd] = LRPath + classNum + "/" + solverType + "/fold" + i + "/bestC/" +  outputFileName + "_" + c;
                predInd++;
                Arrays.copyOfRange(argumentsPred, 0, predInd - 1);
                double[] measures = predict.mainPredict(argumentsPred);
                accuracies.add(measures[0]);
                precisions.add(measures[1]);
                recalls.add(measures[2]);
                fscores.add(measures[3]);
            }

            for(int o = 0; o < accuracies.size(); o++) {
                System.out.println(accuracies.get(o) + " " + precisions.get(o) + " " + recalls.get(o) + " "  + fscores.get(o));
            }

            //System.out.println("- Finished fold " + (i+1) + ", accuracy: " + df3.format( correct / (double)_testData._data.size() ));
            System.out.println("Accuracy:  " + df3.format(Statistics.Avg(accuracies)) + "  +/-  " + df3.format(Statistics.StdError95(accuracies)));
            System.out.println("Precision: " + df3.format(Statistics.Avg(precisions)) + "  +/-  " + df3.format(Statistics.StdError95(precisions)));
            System.out.println("Recall:    " + df3.format(Statistics.Avg(recalls)) + "  +/-  " + df3.format(Statistics.StdError95(recalls)));
            System.out.println("F-Score:   " + df3.format(Statistics.Avg(fscores)) + "  +/-  " + df3.format(Statistics.StdError95(fscores)));
            System.out.println();
        }
    }

    /*
    Prepare temporal splits for test and train and cross-validataion
     */
    public static void prepareTestTrainSplits() throws ParseException, IOException {

        long []splitTimestamps = new long[numOfFolds];
        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        String[] dates = {"Sun Sep 01 00:00:00 +0000 2013", "Tue Oct 01 00:00:00 +0000 2013", "Fri Nov 01 00:00:00 +0000 2013",
                "Sun Dec 01 00:00:00 +0000 2013", "Wed Jan 01 00:00:00 +0000 2014", "Sat Feb 01 00:00:00 +0000 2014",
                "Sat Mar 01 00:00:00 +0000 2014", "Tue Apr 01 00:00:00 +0000 2014", "Thu May 01  00:00:00 +0000 2014",
                "Sun Jun 01 00:00:00 +0000 2014"};
        String dates0 = "Thu Aug 01 00:00:00 +0000 2013";
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        FileWriter fw, fwTest, fwVal, fwAllTrain;
        BufferedWriter bw, bwTest, bwVal, bwAllTrain;
        String [] splitSt; String classFileName = "";
        int trainFileSize = 0,testFileSize = 0, trainValFileSize = 0; long valSplit;

        //make a hashmap of hashtag_dates of all topical hashtags
        String line;
        fileReaderA = new FileReader(path +featurepath + hashtagSetDate);
        bufferedReaderA = new BufferedReader(fileReaderA);
        Map<String, Long> hashtagSetDate = new HashMap<>();
        while ((line = bufferedReaderA.readLine()) != null) {
            splitSt = line.split(",");
            hashtagSetDate.put(splitSt[1], Long.valueOf(splitSt[2]));
        }
        //build test/train data and hashtag lists
        for(int classNum = 2; classNum < 3; classNum++ ) {
            if(classNum == 1)
                classFileName = disasterFileName;
            else if(classNum == 2)
                classFileName = politicFileName;
            for (int i = 0; i < numOfFolds; i++) {
                trainFileSize = 0;testFileSize = 0;trainValFileSize = 0;
                splitTimestamps[i] = format.parse(dates[i]).getTime();
                fileReaderA = new FileReader(path + classFileName);
                bufferedReaderA = new BufferedReader(fileReaderA);
                fw = new FileWriter(path + classNum + "/fold" + i + "/" + trainFileName+ "_t.csv");
                bw = new BufferedWriter(fw);
                fwTest = new FileWriter(path + classNum + "/fold" + i + "/" +  testFileName  + ".csv");
                bwTest = new BufferedWriter(fwTest);
                fwVal = new FileWriter(path + classNum + "/fold" + i + "/" + trainFileName  + "_v.csv");
                bwVal = new BufferedWriter(fwVal);
                fwAllTrain = new FileWriter(path + classNum + "/fold" + i + "/" +  trainFileName + ".csv");
                bwAllTrain = new BufferedWriter(fwAllTrain);
                //WRITE THE HASHTAG LIST BASED ON TIMESTAMP
                String cleanLine = "";
                while ((line = bufferedReaderA.readLine()) != null) {
                    splitSt = line.split(" ");
                    cleanLine = splitSt[0];
                    for (int j = 1; j < splitSt.length - 1; j++) {
                        cleanLine += " " + splitSt[j];
                    }
                    if (Long.valueOf(splitSt[splitSt.length - 1]) <= splitTimestamps[i]) {
                        if(i > 0)
                            valSplit = splitTimestamps[i-1];
                        else
                            valSplit = format.parse(dates0).getTime() ;
                        if(Long.valueOf(splitSt[splitSt.length - 1]) >= valSplit){
                            bwVal.write(cleanLine + "\n");
                            trainValFileSize++;
                        }else {
                            trainFileSize++;
                            bw.write(cleanLine + "\n");
                        }
                        bwAllTrain.write(cleanLine + "\n");
                    }
                    else {
                        testFileSize++;
                        bwTest.write(cleanLine + "\n");
                    }
                }
                bufferedReaderA.close();
                bw.close();
                bwTest.close();
                bwVal.close();
                bwAllTrain.close();
                System.out.println("FileName: " + classFileName + " - TrainFileLine: " + trainFileSize + " - TrainValFileLine: " + trainValFileSize + " - TestFileLine: " + testFileSize);

                //build test/train hashtag lists
                fileReaderA = new FileReader(path + classNum + "/" + allHashtagList + "_" + classNum + ".csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                fw = new FileWriter(path + classNum + "/fold" + i + "/" + trainHashtagList + ".csv");
                bw = new BufferedWriter(fw);
                fwTest = new FileWriter(path + classNum + "/fold" + i + "/" + testHashtagList + ".csv");
                bwTest = new BufferedWriter(fwTest);
                while ((line = bufferedReaderA.readLine()) != null) {
                    if (hashtagSetDate.get(line) <= splitTimestamps[i]) {
                        bw.write(line + "\n");
                    }else{
                        bwTest.write(line + "\n");
                    }
                }
                bw.close();
                bwTest.close();
                bufferedReaderA.close();
                fw = new FileWriter(path + classNum + "/fold" + i + "/" + splitTimestamps[i] + "_" + dates[i] + ".csv");
                bw = new BufferedWriter(fw);
                bw.write(splitTimestamps[i] + "\n");
                bw.write(dates[i] + "\n");
                bw.close();
            }
        }
        for(int i = 0; i < numOfFolds; i++)
            System.out.println(splitTimestamps[i]);
    }

    public static void findTestTrain() throws IOException, ParseException {
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        fileReaderA = new FileReader(path + featurepath + hashtagFileName);
        bufferedReaderA = new BufferedReader(fileReaderA);
        hashtagMap = new HashMap<>();
        String line;
        while ((line = bufferedReaderA.readLine()) != null) {
            hashtagMap.put(line.split(",")[0], Double.valueOf(line.split(",")[1]));
        }
        bufferedReaderA.close();
        /*fileReaderA = new FileReader(path + indexFileName);
        bufferedReaderA = new BufferedReader(fileReaderA);
        indexMap = new HashMap<>();
        while ((line = bufferedReaderA.readLine()) != null) {
            indexMap.put(line.split(",")[0], Double.valueOf(line.split(",")[1]));
        }*/
    }


    public static void findTopicalTest(String fileName, String hashtagListName) throws IOException {
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        String line;
        Set<Double> testHashtagIndexes;
        String[] splits;
        FileWriter fwTest;
        BufferedWriter bwTest;
        for(int classNum = 1; classNum <= numOfTopics; classNum++) {
            for (int i = 0; i < numOfFolds; i++) {
                fileReaderA = new FileReader(path + classNum + "/fold" + i + "/" + hashtagListName +".csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                testHashtagIndexes = new HashSet<>();
                while ((line = bufferedReaderA.readLine()) != null) {
                    testHashtagIndexes.add(hashtagMap.get(line));
                }
                bufferedReaderA.close();
                System.out.println("========================foldNum: " + i + "============================");
                fileReaderA = new FileReader(path + classNum + "/fold" + i + "/" +  fileName  + ".csv");
                bufferedReaderA = new BufferedReader(fileReaderA);
                fwTest = new FileWriter(path + classNum + "/fold" + i + "/" +  fileName  + "_edited.csv");
                bwTest = new BufferedWriter(fwTest);
                while ((line = bufferedReaderA.readLine()) != null) {
                    if(line.length() == 1) {
                        bwTest.write(line);
                        continue;
                    }
                    line = line.substring(2, line.length());
                    splits = line.split(":1 ");
                    splits[splits.length-1] = splits[splits.length-1].split(":1")[0];
                    for(int k = 1; k < splits.length; k++){
                        if(testHashtagIndexes.contains(Double.valueOf(splits[k])))
                            bwTest.write("1 ");
                        else
                            bwTest.write("0 ");
                        bwTest.write(line);
                    }
                }
                bufferedReaderA.close();
                bwTest.close();
            }
        }
    }

    public static void modifyFeatureList() throws IOException {
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
        while ((line = bufferedReaderA.readLine()) != null) {
            if(ind <= 361789)
                bwTest.write("from,"+line+"\n");
            else if(ind <= 317846+361789)
                bwTest.write("term,"+line+"\n");
            else if(ind <= 317846+361789+72267) {
                bwTest.write("hashtag," + line+"\n");
                bw.write(line+"\n");
            }
            else if(ind <= 317846+361789+72267+244478)
                bwTest.write("mention,"+line+"\n");
            ind++;
        }
        bw.close();
        bwTest.close();
        bufferedReaderA.close();
    }
}
