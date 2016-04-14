package functionalGradient;

import ddInference.src.graph.Graph;
import ddInference.src.logic.add_gen.ADD;
import ddInference.src.logic.add_gen.DD;
import ddInference.src.logic.add_gen.FBR;
import functionalGradient.regressionTree.RegTree;
import lucene.FileIndexBuilder;
import lucene.SimpleSearchRanker;
import machinelearning.Feature;
import machinelearning.LearningProblem;
import org.apache.lucene.queryParser.QueryParser;
import util.ConfigRead;
import util.TweetResult;
import util.TweetUtil;

import java.io.*;
import java.util.*;

/**
 * Created by zahraiman on 2/17/16.
 */
public class LearnBooleanQueries {

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
    public static final String trainMethod = "singleRegTree";
    public static BufferedWriter reportWriter;
    public static int validationBestK;
    public static double validationBestC;
    public static double validationBestThreshold;
    public static SimpleSearchRanker simpleSearchRanker;
    public static int validationBestTreeDepth;
    public static final int num_hits = 2000000;

    public static LogisticRegressionProblem lr;
    public static LearningProblem learningProblem;
    public static RegTree regTree;
    public static int[] kValues = new int[]{5, 10, 100, 1000, 10000, 1000000, 1166582};
    public static double[] cValues = {1e-8, 1e-7, 1e-6, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000, 1e4, 1e5, 1e7, 1e8};
    public static int[] treedepthVals = new int[]{3, 5, 7, 10, 15, -1};
    public static HashMap<Integer, Object> depthADD;
    public static HashMap<Integer, ArrayList<String>> depthBranches;

    public static void main(String[] args) throws Exception {
        setFlags(trainMethod);
        tweetUtil = new TweetUtil();
        configRead = new ConfigRead();
        learningProblem = new LearningProblem();
        learningProblem.solverType = "l2_lr";
        reportWriter = new BufferedWriter(new FileWriter(learningProblem.path + "report_"+trainMethod));

        for(int numFeat : new int[]{100, 1000, 10000, 100000, 1166582}) {//100,
            numOfFeatures = numFeat;
            if(numFeat > 1000)
                treedepthVals = new int[]{3, 5, 7, 10, 15, 100};
            lr = new LogisticRegressionProblem(learningProblem, cValues);
            TweetArff tweetArff = new TweetArff(numOfFeatures, pythonArff, liblinearSparse);
            String dataPath, arffDataPath, validDataPath, validArffDataPath, testDataPath = "", testArffDataPath = "";
            String trainName, trainArffName, validName, validArffName, testName, testArffName, filePath;
            LearningProblem.prepareDirectories(new int[]{numOfFeatures});
            reportWriter.write(trainMethod + "," + numOfFeatures + "\n");
            if(trainMethod.contains("RegTree"))
                tweetArff.setTestRegTree(true);

            for (int classInd = 1; classInd < configRead.getNumOfGroups(); classInd++) {
                if (classInd != 9)//) && classInd != 9 && classInd != 6)
                    continue;
                //Split data to train/test and label them based on train/test hashtags
                int trainFileSize, testFileSize;
                String classname = configRead.getGroupNames()[classInd - 1];
                reportWriter.write(classname + "\n");
                learningProblem.getFeatureList(numOfFeatures, classname);
                tweetArff.makeHashtagSets(learningProblem, classInd);
//            order = new ArrayList();
//            order.addAll(learningProblem.featureMap.values());
                _context = new FBR(1, learningProblem.getFeatureOrders()); // 1: ADD
                TweetADD tweetADD = new TweetADD(learningProblem, _context, bigram);
                tweetArff.makeArffTestTrainSplits(learningProblem, classInd);

                filePath = LearningProblem.path + classname + "/fold" + numOfFeatures + "/";
                ArrayList<String> sortedMIFeatures = null;
                trainVal = true;
                Object learnedFun = null;
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
                    String indexName = validName.split(".csv")[0];
                    FileIndexBuilder b = new FileIndexBuilder(configRead.getIndexPath() + "/" + classname + "/" + indexName);
                    if(!new File(configRead.getIndexPath() + "/" + classname + "/" + indexName).exists()) {
                        TweetUtil.runStringCommand("mkdir " + configRead.getIndexPath() + "/" + classname);
                        TweetUtil.runStringCommand("mkdir " + configRead.getIndexPath() + "/" + classname + "/" + indexName);
                        b.buildIndex(validDataPath);
                    }
                    simpleSearchRanker = new SimpleSearchRanker(b._indexPath, "CONTENT", b._analyzer);
                    double f0 = tweetADD.computeF0(dataPath);


                    double prevMAP = -1, prevPrec = -1;
                    depthADD = new HashMap<>();
                    depthBranches = new HashMap<>();
                    for (int iteration = 1; iteration < MAX_ITERATION; iteration++) {
                        double currPrec = 0, currMAP = 0;
                        System.out.println("Iteration: " + iteration);
                        BufferedReader sampleReader = new BufferedReader(new FileReader(dataPath));
                        Object fun = null;

                        if ((makeADDdirectly || bigram || boostedRegTree)) {
                            if (boostedRegTree) {

                                if(!trainVal)
                                    treedepthVals = new int[]{validationBestTreeDepth};
                                for(int treede : treedepthVals) {
                                    learnedFun = tweetADD.trainBoostedRegTree(arffDataPath, filePath, validArffDataPath, iteration, trainFileSize, testFileSize, numOfFeatures, treede, f0);
                                    depthADD.put(treede, learnedFun);
                                }
                            } else {
                                fun = tweetADD.convertTweetsToADD(sampleReader, learnedFun, iteration, classInd, f0);
                                sampleReader.close();
                                fun = _context.scalarMultiply(fun, (1.0 / Math.sqrt(iteration)));
                                if (learnedFun == null)
                                    learnedFun = _context.scalarAdd(fun, f0);
                                else
                                    learnedFun = _context.applyInt(learnedFun, fun, DD.ARITH_SUM);
                            }
                        } else if (singleRegTree) {

                            if(!trainVal)
                                treedepthVals = new int[]{validationBestTreeDepth};
                            for (int treedepthVal : treedepthVals) {
                                String pythonScript = configRead.getPythonPath() + " script/makeSingleDecTree.py " + numOfFeatures + " " + trainFileSize + " " +
                                        testFileSize + " " + arffDataPath + " " + validArffDataPath + " " + treedepthVal + " " + iteration;
                                TweetUtil.runPythonScrip(pythonScript);
                                ArrayList result = RegTree.makeStepTreeFromPythonRes(learningProblem.inverseFeatureMap, "RegTree/treeStruct_" + iteration + "_" + treedepthVal + ".txt", null, true, learningProblem.featureMap);
                                depthBranches.put(treedepthVal, (ArrayList<String>) result.get(0));
                                ArrayList resRegTree = (ArrayList) result.get(1);
//                                learnedFun = _context.buildDDFromUnorderedTree(resRegTree, learningProblem.featureMap);
//                                _context.addSpecialNode(learnedFun);
                                _context.flushCaches(false);
//                                ((ADD)_context._context).collectLeaves((Integer)learnedFun, leafValues);
                                depthADD.put(treedepthVal, learnedFun);
                            }
                        } else if (logisticRegression || topWeightedLR) {
                            lr.trainLogisticRegression(arffDataPath, validArffDataPath, classname, classInd, numOfFeatures, trainVal);
                        } else if (topMI) {
                            sortedMIFeatures = learningProblem.getSortedMIFeatures(classname);
                        }
                        double[] mapP100 = validate(tweetADD, classname, classInd, depthADD, iteration, validDataPath, sortedMIFeatures, trainVal, learnedFun);
                        currMAP = mapP100[0];
                        currPrec = mapP100[1];
                        if (((currMAP == prevMAP && iteration > 50) || (currMAP < prevMAP)) || singleRegTree || logisticRegression || topWeightedLR || topMI) { // MAP Dropping
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

    public static void visualizeGraph(Object dd, String fileName, boolean mapNames){
        Graph g;
        if(mapNames)
            g = _context.getGraph(dd, learningProblem.inverseFeatureMap);
        else
            g = _context.getGraph(dd);
        g.genDotFile(fileName + ".dot");
        //System.out.println(_context.printNode(dd));
        //g.launchViewer(/*width, height*/);
    }

    public static double[] validate(TweetADD tweetADD, String classname, int classInd, HashMap<Integer, Object> depthADD, int iteration, String testDataPath, ArrayList<String> sortedMIFeatures, boolean validataion, Object learnedFun) throws Exception {
        //VALIDATION
        String[] splits;
        BufferedReader sampleReader;
        String tweet;
        int target_label;
        int validInd = 0, tp = 0, fp = 0, tn = 0, fn = 0, index;
        double bestC = -1;
        ArrayList<TweetResult> tweetWeights = null;

        reportWriter.write((validataion) ? "Validation\n" : "Test\n");

        HashSet<Double> leafValues;
        if(makeADDdirectly || bigram) {
            sampleReader = new BufferedReader(new FileReader(testDataPath));
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
                double value = tweetADD.evaluateSampleInADD(learningProblem.featureMap, features, learnedFun);
                System.out.println(validInd + " - " + target_label + " => " + value);
                tweetWeights.add(new TweetResult(validInd, value, tweet, target_label));
                double pPos = 1 / (1 + Math.exp(-2 * value));
                double pNeg = 1 - pPos;

                if (pPos > pNeg) {
                    if (target_label == 1) tp++;
                    else fp++;
                } else {
                    if (target_label == 1) fn++;
                    else tn++;
                }
            }
        }else if(boostedRegTree){
            double bestMap = -1, bestPrec = -1, bestAcc = -1, bestFm = -1;

            double[] thresholds = null;
            if(!trainVal) {
                treedepthVals = new int[]{validationBestTreeDepth};
                thresholds = new double[]{validationBestThreshold};
            }
            for(int treede : treedepthVals) {
                if(trainVal){
                    leafValues = ((ADD)_context._context).collectLeafValues((Integer) depthADD.get(treede));
                    double minVal = Double.MAX_VALUE, maxVal = Double.MIN_VALUE;
                    for(double lv : leafValues){
                        minVal = (lv < minVal)? lv : minVal;
                        maxVal = (lv > maxVal)? lv : maxVal;
                    }
                    thresholds = new double[]{(minVal+maxVal)/10, (minVal+maxVal)/4, (minVal+maxVal)/2, (minVal+maxVal)*3/4, (minVal+maxVal)*9/10};
                }
                for(double threshold : thresholds) {
                    //visualizeGraph(depthADD.get(treede), "learnedFun");
                    Object prunedADD = _context.pruneNodes(depthADD.get(treede), threshold);
                    visualizeGraph(prunedADD, "learnedFun2", true);
                    String query = getQuery(treede);
                    System.out.println("***********************");
                    System.out.println(query);
                    System.out.println("***********************");
                    double[] mapP100;
                    if (query == null || query.length() == 1) {
                        mapP100 = new double[2];
                        mapP100[0] = 0;
                        mapP100[1] = 0;
                    } else{
                        simpleSearchRanker.doSearch(QueryParser.escape(query), num_hits, System.out);
                    }
                    tp = 0;
                    fp = 0;
                    tn = 0;
                    fn = 0;
                    sampleReader = new BufferedReader(new FileReader(testDataPath));
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
                        double value = tweetADD.evaluateSampleInADD(learningProblem.featureMap, features, depthADD.get(treede));
                        System.out.println(validInd + " - " + target_label + " => " + value);
                        tweetWeights.add(new TweetResult(validInd, value, tweet, target_label));
                        double pPos = 1 / (1 + Math.exp(-2 * value));
                        double pNeg = 1 - pPos;

                        if (pPos > pNeg) {
                            if (target_label == 1) tp++;
                            else fp++;
                        } else {
                            if (target_label == 1) fn++;
                            else tn++;
                        }
                    }
                    sampleReader.close();
                    Collections.sort(tweetWeights);
                    mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
                    if (mapP100[0] > bestMap) {
                        bestMap = mapP100[0];
                        bestPrec = mapP100[1];
                        validationBestTreeDepth = treede;
                        validationBestThreshold = threshold;
                    }
                    System.out.println("TP: " + tp + " out of " + validInd);
                    System.out.println("FP: " + fp);
                    System.out.println("TN: " + tn);
                    System.out.println("FN: " + fn);
                    double acc = (double) (tp + tn) / (tp + fp + tn + fn);
                    double pr = (double) (tp) / (tp + fp);
                    double re = (double) (tp) / (tp + fn);
                    double fm = (2 * pr * re) / (pr + re);
                    if (acc > bestAcc)
                        bestAcc = acc;
                    if (fm > bestFm)
                        bestFm = fm;
                }
            }
            reportWriter.write("BestMAP" + "," + "BestP@100" + "," + "bestTreeDepth" + "\n");
            reportWriter.write(bestMap + "," + bestPrec + "," + validationBestTreeDepth + "\n");
            System.out.println("BestMAP: " + bestMap + " bestTreeDepth: " + validationBestTreeDepth + " bestThreshold: " + validationBestThreshold);
            System.out.println("BestAcc: " + bestAcc  + " Best F-Measuer: " + bestFm);
            return new double[]{bestAcc, bestPrec, -1, -1};
        }else if(singleRegTree) {
            double bestMap = -1, bestPrec = -1;
            double[] thresholds = null;

            if(!trainVal) {
                treedepthVals = new int[]{validationBestTreeDepth};
            }
            int sumNotZero = 0;
            for(int treeDe : treedepthVals) {
                tweetWeights = new ArrayList<>();
                //visualizeGraph((Integer) depthADD.get(treeDe), "learnedFun2", true);
//                System.out.println("#Nodes: " + _context.countExactNodes(depthADD.get(treeDe)));
                String query = getQuery(treeDe);
                System.out.println("***********************");
                System.out.println(query);
                System.out.println("***********************");
                double[] mapP100;
                if (query == null || query.length() == 1) {
                    mapP100 = new double[2];
                    mapP100[0] = 0;
                    mapP100[1] = 0;
                } else{
                        mapP100 = validateUsingQuery(query, testDataPath, classname, classInd, iteration, "SingleRegTree");
                    if (mapP100[0] >= bestMap) {
                        bestMap = mapP100[0];
                        bestPrec = mapP100[2];
                        validationBestTreeDepth = treeDe;
                    }
                }
//                sampleReader = new BufferedReader(new FileReader(testDataPath));
//                tweetWeights = new ArrayList<>();
//                while ((tweet = sampleReader.readLine()) != null) {
//                    target_label = 0;
//                    splits = tweet.split(" ");
//                    if (splits[0].split(",")[0].equals("1"))
//                        target_label = 1;
//                    if (splits[1].equals("1")) {// || line.substring(4,5).equals("0")) {//exclude tweets with no hashtag
//                        continue;
//                    }
//                    validInd++;
//                    splits = tweet.split("[ ,]");
//
//                    //Find the F_m-1 (x_i)
//                    ArrayList<String> features = new ArrayList<String>();
//                    int startInd = 3;
//                    for (int i = startInd; i < splits.length - 1; i += 2) {
//                        features.add(splits[i]);
//                    }
//                    double value = tweetADD.evaluateSampleInADD(learningProblem.featureMap, features, depthADD.get(treeDe));
////                    System.out.println(validInd + " - " + target_label + " => " + value);
//                    tweetWeights.add(new TweetResult(validInd, value, tweet, target_label));
//                    double pPos = 1 / (1 + Math.exp(-2 * value));
//                    double pNeg = 1 - pPos;
//
//                    if (pPos > pNeg) {
//                        if (target_label == 1) tp++;
//                        else fp++;
//                    } else {
//                        if (target_label == 1) fn++;
//                        else tn++;
//                    }
//                }
//                sampleReader.close();
//                Collections.sort(tweetWeights);
//                double[] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "SingleRegTree");
//                if (mapP100[0] > bestMap) {
//                    bestMap = mapP100[0];
//                    bestPrec = mapP100[1];
//                    validationBestTreeDepth = treeDe;
//                }
            }
            System.out.println("SumNotZero: " + sumNotZero);
            reportWriter.write("BestMAP" + "," + "BestP@100" + "," + "bestTreeDepth" + "\n");
            reportWriter.write(bestMap + "," + bestPrec + "," + validationBestTreeDepth + "\n");
            System.out.println("BestMAP: " + bestMap + "," + "BestP@100: " + bestPrec + " bestTreeDepth: " + validationBestTreeDepth + " bestThreshold: " + validationBestThreshold);
            return new double[]{bestMap, bestPrec, -1, -1};
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
            StringBuilder query = new StringBuilder("");
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
                        if((lr.isFirstFlagOne() && features.get(i).getFeatureWeight() > 0 && i >= features.size()-k) ||
                                (!lr.isFirstFlagOne() && i < k && features.get(i).getFeatureWeight() < 0)) {
                            features.get(i).setFeatureWeight(1);
                            query.append(features.get(i)).append(" OR ");
                        }else
                            features.get(i).setFeatureWeight(0);
                    }
                    int len = query.length();
                    query.delete(len-5,len-1);
                    double[] mapP100 = validateUsingQuery(query.toString(), testDataPath, classname, classInd, iteration, "SingleRegTree");
//                    tweetWeights = lr.validateModel(classname, testDataPath, numOfFeatures, classInd, cVal, true);
//                    Collections.sort(tweetWeights);
//                    double[] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, "GradientBoosting");
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

    public static String getQuery(int treede){
//        int learnedFun = (Integer) depthADD.get(treede);
        StringBuilder query = new StringBuilder();
        //visualizeGraph(learnedFun, "learnedFun");
        ArrayList<String> branches = depthBranches.get(treede);

//        if(branches == null)
//            branches.addAll(_context.traverseBFSADD((Integer) learnedFun));
        _context.flushCaches(false);
        for(String branch : branches){
            String[] splits = branch.split(" ");
            if(splits.length == 1) {//case that there is only one single node left in ADD
                if (Double.valueOf(splits[splits.length - 1]) == 1.0)
                    return "1";
                else
                    return null;
            }
            if(Double.valueOf(splits[splits.length-1]) == 0.0 || splits.length == 0)
                continue;
            boolean queryAppended = false;
            query.append("(");
            for(int i = 0; i < splits.length; i++){
                String[] feats = splits[i].split(":");
                if(feats.length == 1)
                    continue;
                if(feats[1].equals("false")){
                    continue;
                }else{
                    queryAppended = true;
                    query.append(learningProblem.inverseFeatureMap.get(Integer.valueOf(feats[0])).replaceAll("!", "") + " AND ");
                }
            }
            if(queryAppended) {
                for (int ij = 0; ij < 5; ij++)
                    query.deleteCharAt(query.length() - 1);
                query.append(") OR ");
            }else
                query.deleteCharAt(query.length() - 1);
        }
        for(int ij = 0; ij < 4; ij++)
            query.deleteCharAt(query.length()-1);
        return query.toString();
    }

    public static double[] validateUsingQuery(String query, String testDataPath, String classname, int classInd, int iteration, String methodName) throws Exception {
        ArrayList<TweetResult> tweetWeights = new ArrayList<>();
        PrintStream ps = new PrintStream(new File(testDataPath + "_queryRes.txt"));
        simpleSearchRanker.doSearch(query, num_hits, ps);
        ps.flush();
        ps.close();
        BufferedReader bufferedReader1 = new BufferedReader(new FileReader(testDataPath + "_queryRes.txt"));
        String line = bufferedReader1.readLine();
        int validInd = 0, target_label, tp = 0, tpfp = 0;
        while((line = bufferedReader1.readLine()) != null){
            target_label = 0;
            validInd++;
            String[] splits = line.split("[) ,]");
            if (splits[3].equals("1") && !splits[4].equals("1")) {
                target_label = 1;
                tp++;
            }
            tpfp++;
            tweetWeights.add(new TweetResult(validInd, 1, line, target_label));
        }
        bufferedReader1.close();
        Collections.sort(tweetWeights);
        if(tweetWeights.size() == 0){
            System.out.println("NO TWEET RETURNED");
            return null;
        }
        double prec = (double) tp / tpfp;
        double[] mapP100 = LearningProblem.computePrecisionMAP(tweetWeights, classname, classInd, numOfFeatures, iteration, methodName);
        return new double[]{mapP100[0], mapP100[1], prec};
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