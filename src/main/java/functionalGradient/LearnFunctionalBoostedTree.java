package functionalGradient;

import ddInference.src.graph.Graph;
import ddInference.src.logic.add_gen.DD;
import ddInference.src.logic.add_gen.FBR;
import functionalGradient.regressionTree.RegTree;
import machinelearning.Feature;
import machinelearning.LearningProblem;
import util.ConfigRead;
import util.TweetResult;
import util.TweetUtil;

import java.io.*;
import java.util.*;

/**
 * Created by zahraiman on 2/17/16.
 */
public class LearnFunctionalBoostedTree {

    public static TweetUtil tweetUtil;
    public static ConfigRead configRead;
    public static final int MAX_ITERATION = 10000;
    public static final int MAX_TRAIN = 1500000;
    public static int numOfFeatures;
    public static final int treeDepth = 4;
    public static FBR _context;
    public static boolean makeADDdirectly;
    public static boolean boostedRegTree;
    public static boolean singleRegTree;
    public static boolean logisticRegression;
    public static boolean topWeightedLR;
    public static boolean topMI;
    public static boolean pythonArff;
    public static boolean liblinearSparse;
    public static boolean bigram;
    public static boolean trainVal = true;
    public static final String trainMethod = "logisticRegression";
    public static BufferedWriter reportWriter;
    public static int validationBestK;
    public static double validationBestC;

    public static LogisticRegressionProblem lr;
    public static LearningProblem learningProblem;
    public static RegTree regTree;
    public static int[] kValues = new int[]{5, 10, 100, 1000, 10000, 1000000, 1166582};
    public static double[] cValues = {1e-8, 1e-7, 1e-6, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 1e4, 1e5, 1e7, 1e8};

    public static void main(String[] args) throws Exception {
        setFlags(trainMethod);
        tweetUtil = new TweetUtil();
        configRead = new ConfigRead();
        learningProblem = new LearningProblem();
        learningProblem.solverType = "l2_lr";
        reportWriter = new BufferedWriter(new FileWriter(learningProblem.path + "report_"+trainMethod));
        for(int numFeat : new int[]{100, 1000, 10000, 100000, 1166582}) {
            numOfFeatures = numFeat;
            lr = new LogisticRegressionProblem(learningProblem, cValues);

            TweetToArff tweetToArff = new TweetToArff(numOfFeatures, pythonArff, liblinearSparse);
            String dataPath, arffDataPath, validDataPath, validArffDataPath, testDataPath = "", testArffDataPath = "";
            String trainName, trainArffName, validName, validArffName, testName, testArffName, filePath;
            LearningProblem.prepareDirectories(new int[]{numOfFeatures});
            reportWriter.write(trainMethod + "," + numOfFeatures + "\n");


            for (int classInd = 1; classInd < configRead.getNumOfGroups(); classInd++) {
                if (classInd != 1 && classInd != 6 && classInd != 9)
                    continue;
                //Split data to train/test and label them based on train/test hashtags
                int trainFileSize, testFileSize;
                String classname = configRead.getGroupNames()[classInd - 1];
                reportWriter.write(classname + "\n");
                learningProblem.getFeatureList(numOfFeatures, classname);
                tweetToArff.makeHashtagSets(learningProblem, classInd);
//            order = new ArrayList();
//            order.addAll(learningProblem.featureMap.values());
                _context = new FBR(1, learningProblem.getFeatureOrders()); // 1: ADD
                TweetADD tweetADD = new TweetADD(learningProblem, _context, bigram);
                tweetToArff.makeArffTestTrainSplits(learningProblem, classInd);

                filePath = LearningProblem.path + classname + "/fold" + numOfFeatures + "/";
                ArrayList<String> sortedMIFeatures = null;
                trainVal = true;
                for (int tv = 0; tv < 2; tv++) {//TRAIN_VAL / TRAIN_TEST
                    if (trainVal) {
                        trainName = LearningProblem.trainFileName + "_t_strings.csv";
                        trainArffName = LearningProblem.trainFileName + "_t.arff";
                        validName = LearningProblem.trainFileName + "_v_strings.csv";
                        validArffName = LearningProblem.trainFileName + "_v.arff";
                        trainFileSize = learningProblem.getTrainFileSize()[classInd - 1];
                        testFileSize = learningProblem.getTrainValFileSize()[classInd - 1];
                    } else {
                        trainName = LearningProblem.trainFileName + "_strings.csv";
                        trainArffName = LearningProblem.trainFileName + ".arff";
                        validName = LearningProblem.testFileName + "_strings.csv";
                        validArffName = LearningProblem.testFileName + ".arff";
                        trainFileSize = learningProblem.getTrainFileSize()[classInd - 1] + learningProblem.getTrainValFileSize()[classInd - 1];
                        testFileSize = learningProblem.getTestFileSize()[classInd - 1];
                    }
                    dataPath = filePath + trainName;//.arff";
                    arffDataPath = filePath + trainArffName;
                    validDataPath = filePath + validName;
                    validArffDataPath = filePath + validArffName;
                    double f0 = tweetADD.computeF0(dataPath);

                    double prevMAP = -1, prevPrec = -1;
                    for (int iteration = 1; iteration < MAX_ITERATION; iteration++) {
                        double currPrec = 0, currMAP = 0;
                        System.out.println("Iteration: " + iteration);
                        BufferedReader sampleReader = new BufferedReader(new FileReader(dataPath));
                        Object fun = null, prevFun = null;

                        if ((makeADDdirectly || bigram || boostedRegTree)) {
                            if (boostedRegTree) {
                                tweetADD.trainBoostedRegTree(arffDataPath, filePath, testArffDataPath, iteration, trainFileSize, testFileSize, numOfFeatures, treeDepth);
                            } else {
                                fun = tweetADD.convertTweetsToADD(sampleReader, prevFun, iteration, classInd, f0);
                                sampleReader.close();
                            }
                            fun = _context.scalarMultiply(fun, (1.0 / Math.sqrt(iteration)));
                            if (prevFun == null)
                                prevFun = _context.scalarAdd(fun, f0);
                            else
                                prevFun = _context.applyInt(prevFun, fun, DD.ARITH_SUM);
                        } else if (singleRegTree) {
                            TweetUtil.runStringCommand("python script/makeSingleRegTree.py " + numOfFeatures + " " + trainFileSize + " " +
                                    testFileSize + " " + arffDataPath + " " + validArffDataPath + " " + -1 + " " + iteration);
                            ArrayList resRegTree = RegTree.makeStepTreeFromPythonRes(learningProblem.inverseFeatureMap, "RegTree/treeStruct_" + iteration + ".txt");
                            fun = null;
                        } else if (logisticRegression || topWeightedLR) {
                            lr.trainLogisticRegression(arffDataPath, validArffDataPath, classname, classInd, numOfFeatures, trainVal);
                        } else if (topMI) {
                            sortedMIFeatures = learningProblem.getSortedMIFeatures(classname);
                        }
                        double[] mapP100 = validate(tweetADD, classname, classInd, fun, iteration, validDataPath, sortedMIFeatures, trainVal);
                        currMAP = mapP100[0];
                        currPrec = mapP100[1];
                        if (currMAP < prevMAP || singleRegTree || logisticRegression || topWeightedLR || topMI) { // MAP Dropping
                            break;
                        }
                        sampleReader.close();
                        prevMAP = currMAP;
                        reportWriter.flush();
                    }
                    trainVal = false;
                }
                //TEST
            /*mapP100 = validate(tweetADD, classname, classInd, fun, -1, testDataPath, testArffDataPath);
            System.out.println("MAP: " + mapP100[0]);
            System.out.println("P@100: " + mapP100[1]);*/
                reportWriter.flush();
            }
        }
        reportWriter.close();
    }

    public static void visualizeGraph(Object dd, String fileName){
        Graph g = _context.getGraph(dd);
        g.genDotFile(fileName + ".dot");
        //System.out.println(_context.printNode(dd));
        //g.launchViewer(/*width, height*/);
    }

    public static double[] validate(TweetADD tweetADD, String classname, int classInd, Object fun, int iteration, String testDataPath, ArrayList<String> sortedMIFeatures, boolean validataion) throws Exception {
        //VALIDATION
        String[] splits;
        BufferedReader sampleReader = new BufferedReader(new FileReader(testDataPath));
        String tweet;
        int target_label;
        int validInd = 0, tp = 0, fp = 0, tn = 0, fn = 0, index;
        double bestC = -1;
        ArrayList<TweetResult> tweetWeights = null;

        reportWriter.write((validataion) ? "Validation\n" : "Test\n");

        if(makeADDdirectly || boostedRegTree || bigram) {
            tweetWeights = new ArrayList<>();
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
                int startInd = 3;
                for (int i = startInd; i < splits.length - 1; i += 2) {
                    features.add(splits[i]);
                }
                double value = tweetADD.evaluateSampleInADD(learningProblem.featureMap, features, fun);
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
            tweetWeights = new ArrayList<>();
            //tweetWeights = regTree.evaluateModel(testArffDataPath);
            BufferedReader bufferedReader = new BufferedReader(new FileReader("RegTree/predictions_"+iteration+".txt"));
            BufferedReader bufferedReader1 = new BufferedReader(new FileReader(testDataPath));
            String line;
            validInd = 0;
            while((line = bufferedReader.readLine()) != null){
                splits = line.split(" ");
                tweetWeights.add(new TweetResult(validInd, Double.valueOf(splits[1]), bufferedReader1.readLine(), Integer.valueOf(splits[0])));
                validInd++;
            }
            bufferedReader.close();
            bufferedReader1.close();
        }else if(logisticRegression){
            double bestMap = -1, bestPrec = -1;
            bestC = -1;
            double[] _cValues = cValues;
            if(!validataion) {
                _cValues = new double[]{validationBestC};
            }
            for (double lambda : _cValues) {
                tweetWeights = lr.validateModel(classname, testDataPath, numOfFeatures, classInd, lambda, false);
                Collections.sort(tweetWeights);
                double[] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
                if(mapP100[0] > bestMap) {
                    bestMap = mapP100[0];
                    bestPrec = mapP100[1];
                    bestC = lambda;
                }
            }
            validationBestC = bestC;
            reportWriter.write("BestMAP" + "," + "BestP@100" + "," + "bestC" + "\n");
            reportWriter.write(bestMap + "," + bestPrec + "," + bestC + "\n");
            return new double[]{bestMap, bestPrec, -1, bestC};
        }else if(topWeightedLR){
            double bestMap = -1, bestPrec = -1;
            int bestK = -1;
            ArrayList<Feature> features;
            int[] _kValues = kValues;
            double[] _cValues = cValues;
            if(!validataion) {
                _kValues = new int[]{validationBestK};
                _cValues = new double[]{validationBestC};
            }
            for(double cVal : _cValues) {
                for (int k : _kValues) {
                    features = lr.getFeatures(cVal);//TODO should fix it for the case of neg features if(firstFlag)
                    Collections.sort(features);
                    for (int i = 0; i < features.size() ; i++) {
                        if((lr.isFirstFlagOne() && i >= features.size()-k) || (!lr.isFirstFlagOne() && i < k))
                            features.get(i).setFeatureWeight(1);
                        else
                            features.get(i).setFeatureWeight(0);
                    }
                    tweetWeights = lr.validateModel(classname, testDataPath, numOfFeatures, classInd, cVal, true);
                    Collections.sort(tweetWeights);
                    double[] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
                    if(mapP100[0] > bestMap) {
                        bestMap = mapP100[0];
                        bestPrec = mapP100[1];
                        bestK = k;
                        bestC = cVal;
                    }
                }
            }
            validationBestC = bestC; validationBestK = bestK;
            System.out.println("BestMAP: " + bestMap + " bestK: " + bestK + " bestC: " + bestC);
            reportWriter.write("BestMAP" + "," + "BestP@100" + "," + "bestK" + "," + "bestC" + "\n");
            reportWriter.write(bestMap + "," + bestPrec + "," + bestK + "," + bestC + "\n");
            return new double[]{bestMap, bestPrec, bestK, bestC};
        }else if(topMI){
            double bestMap = -1, bestPrec = -1;
            int bestK = -1;
            ArrayList<Feature> features = new ArrayList<>();
            int[] _kValues = kValues;
            if(!validataion)
                _kValues = new int[]{validationBestK};
            for (int k : _kValues) {
                for (int i = 0; i < sortedMIFeatures.size() ; i++) {
                    if(i < k)
                        features.add(new Feature(sortedMIFeatures.get(i), 1));
                    else
                        features.add(new Feature(sortedMIFeatures.get(i), 0));
                }
                validationBestC = bestC; validationBestK = bestK;
                lr.setFeatures(features, 1);
                tweetWeights = lr.validateModel(classname, testDataPath, numOfFeatures, classInd, 1, true);
                Collections.sort(tweetWeights);
                double[] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
                if(mapP100[0] > bestMap) {
                    bestMap = mapP100[0];
                    bestPrec = mapP100[1];
                    bestK = k;
                }
            }
            reportWriter.write("BestMAP" + "," + "BestP@100" + "," + "bestK" + "\n");
            reportWriter.write(bestMap + "," + bestPrec + "," + bestK + "\n");
            System.out.println("BestMAP: " + bestMap + " bestK: " + bestK);
            return new double[]{bestMap, bestPrec, bestK, -1};
        }
        Collections.sort(tweetWeights);
        double [] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
        reportWriter.write("BestMAP" + "," + "BestP@100" + "," + "iteration" + "\n");
        reportWriter.write(mapP100[0] + "," + mapP100[1] + "," + iteration + "\n");
        System.out.println("Iteration: " + iteration + " - MAP: " + mapP100[0] + " P@100: " + mapP100[1]);
        System.out.println("TP: " + tp + " out of " + validInd);
        System.out.println("FP: " + fp);
        System.out.println("TN: " + tn);
        System.out.println("FN: " + fn);
        return new double[]{mapP100[0], mapP100[1], -1, bestC};
    }



    public static void setFlags(String trainMethod) {
        switch (trainMethod) {
            case ("makeADDdirectly"):// (makeADDdirectly || bigram || boostedRegTree)
                pythonArff = true;
                makeADDdirectly = true;
                break;
            case ("bigram"):
                pythonArff = true;
                bigram = true;
                break;
            case ("boostedRegTree"):
                pythonArff = true;
                boostedRegTree = true;
                break;
            case ("singleRegTree"):
                pythonArff = true;
                singleRegTree = true;
                break;
            case ("logisticRegression"):
                liblinearSparse = true;
                logisticRegression = true;
                break;
            case ("topWeightedLR"):
                topWeightedLR = true;
                liblinearSparse = true;
                break;
            case ("topMI"):
                topMI = true;
                break;
        }
    }
}
/*copyADD = fun;
                copyADD = _context.applyInt(copyADD, -1, DD.ARITH_ABS);
                Object power2ADD = _context.applyInt(copyADD, -1, DD.ARITH_POW);
                Object times2ADD = _context.scalarMultiply(copyADD, 2.0d);
                copyADD = _context.applyInt(times2ADD, power2ADD, DD.ARITH_MINUS);
                copyADD = _context.applyInt(fun, copyADD, DD.ARITH_DIV);
                fun = _context.applyInt(fun, copyADD, DD.ARITH_PROD);*/
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