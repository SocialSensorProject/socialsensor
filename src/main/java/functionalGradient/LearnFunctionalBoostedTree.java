package functionalGradient;

import ddInference.src.graph.Graph;
import ddInference.src.logic.add_gen.ADD;
import ddInference.src.logic.add_gen.DD;
import ddInference.src.logic.add_gen.FBR;
import functionalGradient.regressionTree.RegTree;
import functionalGradient.regressionTree.RegressionProblem;
import javafx.util.Pair;
import machinelearning.LearningProblem;
import util.ConfigRead;
import util.TweetResult;
import util.TweetUtil;
import weka.classifiers.Evaluation;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.*;

/**
 * Created by zahraiman on 2/17/16.
 */
public class LearnFunctionalBoostedTree {

    public static TweetUtil tweetUtil;
    public static ConfigRead configRead;
    public static final int MAX_ITERATION = 10000;
    public static final int MAX_TRAIN = 1500000;
    public static final int numOfFeatures = 1000;//1166582;
    public static final int treeDepth = 4;
    public static FBR _context;
    public static boolean makeADDdirectly = false;
    public static boolean boostedRegTree = false;
    public static boolean singleRegTree = true;
    public static boolean pythonArff = true;
    public static boolean bigram = false;
    public static boolean trainVal = true;
    public static LearningProblem learningProblem;
    public static RegTree regTree;

    public static void main(String[] args) throws Exception {
        tweetUtil = new TweetUtil();
        configRead = new ConfigRead();
        learningProblem = new LearningProblem();
        TweetToArff tweetToArff = new TweetToArff(numOfFeatures, pythonArff);
        String dataPath, arffDataPath, validDataPath, validArffDataPath;
        String testDataPath = "";
        String testArffDataPath = "";
        Pair<ArrayList, HashMap> treeVars;
        ArrayList order;
        BufferedReader sampleReader;
        String[] splits;
        TweetToADD tweetToADD;
        Object fun = null, prevFun = null;
        BufferedWriter bufferedWriter, bufferedWriter2;
        LearningProblem.prepareDirectories(new int[]{numOfFeatures});
        double currPrec = 0, prevPrec = 0, currMAP = 0, prevMAP = 0;
        double[] mapP100 = new double[2];
        Object copyADD, emptyADD;
        double f0, mean = 0;
        int trainFileSize, testFileSize;
        String trainName, trainArffName, validName, validArffName, testName, testArffName, filePath;

        for (int classInd = 1; classInd < 2/*configRead.getNumOfGroups()*/; classInd++) {
            // Read train_train data
            regTree = new RegTree();
            String classname = configRead.getGroupNames()[classInd - 1];
            learningProblem.getFeatureList(numOfFeatures, classname);
            tweetToArff.makeHashtagSets(learningProblem, classInd);
            order = new ArrayList();
            order.addAll(learningProblem.featureMap.values());
            _context = new FBR(1, learningProblem.getFeatureOrders()); // 1: ADD
            tweetToADD = new TweetToADD(learningProblem, _context, bigram);
            tweetToArff.makeArffTestTrainSplits(learningProblem, classInd);
            filePath = LearningProblem.path + classname + "/fold" + numOfFeatures + "/";
            for(int tv = 0; tv < 2; tv++) {
                if (trainVal) {
                    trainName = learningProblem.trainFileName + "_t_strings.csv";
                    trainArffName = learningProblem.trainFileName + "_t.arff";
                    validName = learningProblem.trainFileName + "_v_strings.csv";
                    validArffName = learningProblem.trainFileName + "_v.arff";
                    testName = learningProblem.trainFileName + "_v_strings.csv";
                    testArffName = learningProblem.trainFileName + "_v.arff";
                    trainFileSize = learningProblem.getTrainFileSize()[classInd - 1];
                    testFileSize = learningProblem.getTrainValFileSize()[classInd - 1];
                } else {
                    trainName = learningProblem.trainFileName + "_strings.csv";
                    trainArffName = learningProblem.trainFileName + ".arff";
                    validName = learningProblem.trainFileName + "_v_strings.csv";
                    validArffName = learningProblem.trainFileName + "_v.arff";
                    testName = learningProblem.testFileName + "_strings.csv";
                    testArffName = learningProblem.testFileName + ".arff";
                    trainFileSize = learningProblem.getTrainFileSize()[classInd - 1] + learningProblem.getTrainValFileSize()[classInd - 1];
                    testFileSize = learningProblem.getTestFileSize()[classInd - 1];
                }
                dataPath = filePath + trainName;//.arff";
                arffDataPath = filePath + trainArffName;
                testDataPath = filePath + testName;
                testArffDataPath = filePath + testArffName;
                validDataPath = filePath + validName;
                validArffDataPath = filePath + validArffName;
                f0 = computeF0(dataPath);

                for (int iteration = 1; iteration < MAX_ITERATION; iteration++) {
                    System.out.println("Iteration: " + iteration);
                    sampleReader = new BufferedReader(new FileReader(dataPath));
                    if (makeADDdirectly || bigram) {
                        fun = tweetToADD.convertTweetsToADD(sampleReader, prevFun, iteration, classInd, f0);
                        fun = _context.scalarMultiply(fun, (1.0 / Math.sqrt(iteration)));
                        if (prevFun == null)
                            fun = _context.scalarAdd(fun, f0);
                        else
                            fun = _context.applyInt(prevFun, fun, DD.ARITH_SUM);
                        sampleReader.close();
                        prevFun = fun;
                    } else if (boostedRegTree) {
                        //Build Regression Tree
                        //treeVars = regTree.buildRegTree(dataPath, treeDepth);
                        tweetUtil.runStringCommand("python script/makeSingleRegTree.py " + numOfFeatures + " " + trainFileSize + " " +
                                testFileSize + " " + arffDataPath + " " + testArffDataPath + " " + -1 + " " + iteration);
                        ArrayList resRegTree = regTree.makeStepTreeFromPythonRes(null, "RegTree/treeStruct_" + iteration + ".txt");
                        //Build ADD from the tree
                        fun = _context.buildDDFromUnorderedTree(resRegTree, learningProblem.featureMap);
                    } else if (singleRegTree) {
                        tweetUtil.runStringCommand("python script/makeSingleRegTree.py " + numOfFeatures + " " + trainFileSize + " " +
                                testFileSize + " " + arffDataPath + " " + testArffDataPath + " " + -1 + " " + iteration);
                        ArrayList resRegTree = regTree.makeStepTreeFromPythonRes(null, "RegTree/treeStruct_" + iteration + ".txt");
                        fun = null;
                        //fun = _context.buildDDFromUnorderedTree(resRegTree, learningProblem.featureMap);
                        //Pair<ArrayList, HashMap> resRegTree = regTree.buildSingleRegTree(arffDataPath, treeDepth);
                    }
                    mapP100 = validate(tweetToADD, classname, classInd, fun, iteration, validDataPath, validArffDataPath);
                /*copyADD = fun;
                copyADD = _context.applyInt(copyADD, -1, DD.ARITH_ABS);
                Object power2ADD = _context.applyInt(copyADD, -1, DD.ARITH_POW);
                Object times2ADD = _context.scalarMultiply(copyADD, 2.0d);
                copyADD = _context.applyInt(times2ADD, power2ADD, DD.ARITH_MINUS);
                copyADD = _context.applyInt(fun, copyADD, DD.ARITH_DIV);
                fun = _context.applyInt(fun, copyADD, DD.ARITH_PROD);*/

                    currPrec = mapP100[1];
                    currMAP = mapP100[0];

                    if (currMAP < prevMAP || singleRegTree) { // MAP Dropping
                        break;
                    }
                    sampleReader.close();
                    prevMAP = currMAP;
                }
                trainVal = false;
            }
            //TEST
            /*mapP100 = validate(tweetToADD, classname, classInd, fun, -1, testDataPath, testArffDataPath);
            System.out.println("MAP: " + mapP100[0]);
            System.out.println("P@100: " + mapP100[1]);*/
        }
    }


    private static double computeF0(String dataPath) throws IOException {
        String tweet;
        String[] splits;
        double mean = 0;
        int ind = 0;
        BufferedReader sampleReader = new BufferedReader(new FileReader(dataPath));

        while ( (tweet = sampleReader.readLine()) != null) {
            splits = tweet.split(",")[0].split(" ");
            mean += Double.valueOf(splits[splits.length - 1]); // Read label first
            ind++;
        }
        mean /= ind;
        sampleReader.close();
        return (0.5 * Math.log((1+mean)/(1-mean)));
    }

    public static void visualizeGraph(Object dd, String fileName){
        Graph g = _context.getGraph(dd);
        g.genDotFile(fileName + ".dot");
        //System.out.println(_context.printNode(dd));
        //g.launchViewer(/*width, height*/);
    }

    public static double[] validate(TweetToADD tweetToADD, String classname, int classInd, Object fun, int iteration, String testDataPath, String testArffDataPath) throws Exception {
        //VALIDATION
        String[] splits;


        BufferedReader sampleReader = new BufferedReader(new FileReader(testDataPath));
        String tweet;
        int target_label;
        int validInd = 0, tp = 0, fp = 0, tn = 0, fn = 0, index;
        List<TweetResult> tweetWeights = new ArrayList<>();

        if(makeADDdirectly || boostedRegTree || bigram) {
            while ((tweet = sampleReader.readLine()) != null) {
                target_label = 0;
                splits = tweet.split(" ");
                if (splits[0].split(",")[0].equals("1"))
                    target_label = 1;
                if (splits[1].equals("1")) {// || line.substring(4,5).equals("0")) {//exclude tweets with no hashtag
                    continue;
                }
                validInd++;
                splits = tweet.split("[ ,]");

                //Find the F_m-1 (x_i)
                ArrayList<String> features = new ArrayList<String>();
                for (int i = 1; i < splits.length - 1; i += 2) {
                    features.add(splits[i]);
                }
                double value = tweetToADD.evaluateSampleInADD(learningProblem.featureMap, features, fun);
                System.out.println(validInd + " - " + target_label + " => " + value);
                tweetWeights.add(new TweetResult(validInd, value, "", target_label));
                double pPos = 1 / (1 + Math.exp(-2 * value));
                double pNeg = 1 - pPos;

                if (pPos >= pNeg) {
                    if (target_label == 1) tp++;
                    else fp++;
                } else {
                    if (target_label == 1) fn++;
                    else tn++;
                }

            }
        }else if(singleRegTree) {
            //tweetWeights = regTree.evaluateModel(testArffDataPath);
            BufferedReader bufferedReader = new BufferedReader(new FileReader("RegTree/predictions_"+iteration+".txt"));
            String line;
            validInd = 0;
            while((line = bufferedReader.readLine()) != null){
                splits = line.split(" ");
                tweetWeights.add(new TweetResult(validInd, Double.valueOf(splits[1]), "", Integer.valueOf(splits[0])));
                validInd++;
            }
            bufferedReader.close();
        }
        Collections.sort(tweetWeights);
        double [] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
        System.out.println("Iteration: " + iteration + " - MAP: " + mapP100[0] + " P@100: " + mapP100[1]);
        System.out.println("TP: " + tp + " out of " + validInd);
        System.out.println("FP: " + fp);
        System.out.println("TN: " + tn);
        System.out.println("FN: " + fn);
        return mapP100;
    }
}
/*
                readFlag = false;
                sampleTerminalValue = new HashMap<>();   // X \in R_jm
                terminalNodeValues = new HashMap<>();   // R_jm
                terminalNodeUpdates = new HashMap<>(); // gamma_jm for each iteration and terminal label \in {-1, +1}
                //Build Regression Tree
                treeVars = RegTree.buildRegTree(dataPath, treeDepth);
                //Build ADD from the tree
                Object ADD = _context.buildDDFromUnorderedTree(treeVars.getKey(), learningProblem.featureMap);

                // For each train tweet, evaluate which branch it ends up
                int sampleInd = 0;
                while ( (line = sampleReader.readLine()) != null){
                    ArrayList<Integer> features = new ArrayList<Integer>();
                    if(!readFlag) {
                        if (line.equals("@data"))
                            readFlag = true;
                        continue;
                    }
                    splits = line.split(",")[0].split(" ");
                    double topicalLabel = Double.valueOf(splits[splits.length-1]); // Read label first
                    //line = line.substring(5);// read features after the label
                    splits = line.split("[ ,}]");
                    for(int i = 2; i < splits.length; i+=2){
                        features.add(Integer.valueOf(splits[i]));
                    }
                    double tValue = evaluateSampleInADD(learningProblem.featureMap, features, ADD);
                    ArrayList<Double> values = terminalNodeValues.get(tValue);
                    if(values == null){
                        values = new ArrayList<>();
                        terminalNodeValues.put(tValue, values);
                    }
                    values.add(topicalLabel);
                    sampleTerminalValue.put(sampleInd, topicalLabel);
                    sampleInd++;
                }
                //For each terminal node, compute the gamma updating value
                ArrayList sumValues = new ArrayList();
                for(double tValue : terminalNodeValues.keySet()){
                    double num = 0, den= 0;
                    for(double value : terminalNodeValues.get(tValue)) {
                        num += value;
                        den += Math.abs(value) * (2 - Math.abs(value));
                    }
                    terminalNodeUpdates.put(tValue, (num/den));
                }*/