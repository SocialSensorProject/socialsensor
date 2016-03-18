package functionalGradient;

import machinelearning.LearningProblem;
import machinelearning.Feature;
import machinelearning.logisticRegression.de.bwaldvogel.liblinear.InvalidInputDataException;
import machinelearning.logisticRegression.de.bwaldvogel.liblinear.Predict;
import machinelearning.logisticRegression.de.bwaldvogel.liblinear.Train;
import util.TweetResult;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by zahraiman on 3/9/16.
 */
public class LogisticRegressionProblem {
    public static LearningProblem learningProblem;
    private static boolean firstFlagOne;
    double[] cValues;
    private static ArrayList<Feature>[] features;

    public boolean isFirstFlagOne() {
        return firstFlagOne;
    }

    public void setFirstFlagOne(boolean firstFlagOne) {
        LogisticRegressionProblem.firstFlagOne = firstFlagOne;
    }

    public ArrayList<Feature> getFeatures(double cVal) {
        int ind = findCIndicator(cVal);
        if(ind == -1)
            return null;
        return features[ind];
    }

    public int findCIndicator(double cVal) {
        for (int c = 0; c < cValues.length; c++) {
            if (cValues[c] == cVal) {
                return c;
            }
        }
        return -1;
    }

    public void setFeatures(ArrayList<Feature>[] features) {
        LogisticRegressionProblem.features = features;
    }

    public void setFeatures(ArrayList<Feature> features, double cVal) {
        LogisticRegressionProblem.features[findCIndicator(cVal)] = features;
    }

    public LogisticRegressionProblem(LearningProblem _lp, double[] _cvalues){
        cValues = _cvalues;
        learningProblem = _lp;
        features = new ArrayList[cValues.length];
    }

    public ArrayList<TweetResult> validateModel(String classname, String testArffDataPath, int numOfFeatures, int classInd, double lambda, boolean booleanIndicator) throws IOException {
        ArrayList<TweetResult> tweetWeights = new ArrayList<>();
        //BufferedReader bufferedReader = new BufferedReader(new FileReader(LearningProblem.path + classname + "/fold" + numOfFeatures + "/val/" + "l2_lr" + "/featureWeights_" + lambda + ".csv"));
        int trainFeaturesNormVal = 0;
        int target_label;
        String line, feat;
        boolean tweetFlag;
        long tid = 0;
        double featWeight, featNormVal;
        String[] splits;
        HashMap<String, Double> featureWeights = new HashMap<>();
        for(Feature fe : features[findCIndicator(lambda)]){
            featureWeights.put(fe.getFeatureName(), Double.valueOf(fe.getFeatureWeight()));
        }
        //bufferedReader.close();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(testArffDataPath));
        HashSet<String> trainHashtags = new HashSet<>();
        HashSet<String> testHashtags = new HashSet<>();
        HashSet<String> trainTrainHashtags = new HashSet<>();
        HashSet<String> trainValHashtags = new HashSet<>();
        learningProblem.MakeHashtagLists(LearningProblem.classNames[classInd - 1], trainHashtags, trainTrainHashtags, testHashtags, trainValHashtags, numOfFeatures);
        while ((line = bufferedReader.readLine()) != null) {
            tweetFlag = true;
            target_label = 0;
            featWeight = 0;
            featNormVal = 0;
            if (line.substring(0, 1).equals("1"))
                target_label = 1;
            if (line.substring(2, 3).equals("1") ){ //|| line.substring(4, 5).equals("0")) {
                tweetFlag = false;
                continue;
            }
            line = line.substring(6, line.length());
            splits = line.split(" ");
            //tid = Long.valueOf(splits[splits.length - 1]);
            for (int ij = 0; ij < splits.length - 2; ij++) {
                feat = splits[ij].toLowerCase();
                if (featureWeights.containsKey(feat)) {
                    featWeight += featureWeights.get(feat);
                    featNormVal += featureWeights.get(feat) * featureWeights.get(feat);
                }
            }
            if (!tweetFlag)
                continue;
            if(!firstFlagOne && !booleanIndicator)
                featWeight = -featWeight;
            tweetWeights.add(new TweetResult(tid, featWeight, line, target_label));
            tid++;
        }
        return tweetWeights;
    }

    public double trainLogisticRegression(String trainPath, String testPath, String classname, int classInd, int numOfFeatures, boolean trainVal) throws IOException, InterruptedException, InvalidInputDataException {
        Train train = new Train();
        setFirstFlagOneVal(trainPath);
        String[] arguments = new String[50];
        Predict predict = new Predict();
        String[] argumentsPred = new String[50];
        int ind = 0, predInd = 0, cInd = 0;
        double d;

        arguments[ind] = "-s";ind++;
        arguments[ind] = "0";ind++;
        arguments[ind] = "-B";ind++;
        arguments[ind] = "1";ind++;
        argumentsPred[predInd] = "-b";
        predInd++;
        argumentsPred[predInd] = "1";
        predInd++;
        int remInd = ind, remPredInd = predInd;
        double bestc = -1, bestK = -1, bestMap = -1, bestKMap = -1, bestAccuracyC = 0, bestAccuracy;
        for (double c : cValues) {
            ind = remInd;
            predInd = remPredInd;
            System.out.println("========================C Value: " + c + "============================");
            arguments[ind] = "-w0";
            ind++;
            arguments[ind] = String.valueOf(c);
            ind++;
            arguments[ind] = "-w1";
            ind++;
            //arguments[ind] = String.valueOf(c);ind++;
            if(trainVal)
                d = ((double) learningProblem.getTotal()[classInd-1] - learningProblem.getPositives()[classInd-1]) / learningProblem.getPositives()[classInd-1];
            else
                d = ((double) (learningProblem.getTotal()[classInd-1]+learningProblem.getTotalVal()[classInd-1]) - (learningProblem.getPositives()[classInd-1]+learningProblem.getPositivesVal()[classInd-1])) / (learningProblem.getPositives()[classInd-1]+learningProblem.getPositives()[classInd-1]);
            arguments[ind] = String.valueOf(c * d);
            ind++;
            arguments[ind] = trainPath;
            ind++;
            arguments[ind] = LearningProblem.LRPath + classname + "/" + "l2_lr" + "/fold" + numOfFeatures + "/" + LearningProblem.modelFileName + "_" + c;
            ind++;
            Arrays.copyOfRange(arguments, 0, ind - 1);
            train.run(arguments);
            features[cInd] = LearningProblem.writeFeatureFile(classname, LearningProblem.LRPath + classname + "/" + "l2_lr" + "/fold" + numOfFeatures + "/" + LearningProblem.modelFileName + "_" + c, numOfFeatures, LearningProblem.path + LearningProblem.featurepath + LearningProblem.indexFileName + "_" + classname + "_" + numOfFeatures + ".csv", "val", c);

            argumentsPred[predInd] = testPath;
            predInd++;
            argumentsPred[predInd] = LearningProblem.LRPath + classname + "/" + "l2_lr" + "/fold" + numOfFeatures + "/" + LearningProblem.modelFileName + "_" + c;
            predInd++;
            argumentsPred[predInd] = LearningProblem.LRPath + classname + "/" + "l2_lr" + "/fold" + numOfFeatures + "/" + LearningProblem.outputFileName + "_" + c;
            predInd++;
            Arrays.copyOfRange(argumentsPred, 0, predInd - 1);

            double[] measures = predict.mainPredict(argumentsPred);
            //if (measures[3] > bestError) { //error
            if (measures[0] > bestMap) {
                bestAccuracyC = c;
                bestAccuracy = measures[0];
            }
            cInd++;
        }
        return bestAccuracyC;
    }

    public void setFirstFlagOneVal(String trainPath) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(trainPath));
        String line = bufferedReader.readLine();
        firstFlagOne = Integer.valueOf(line.split(" ")[0]) == 1;
    }
}
