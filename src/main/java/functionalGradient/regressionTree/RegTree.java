package functionalGradient.regressionTree;

/**
 * Created by zahraiman on 1/29/16.
 */

import java.io.*;
import java.math.BigDecimal;
import java.util.*;

import ddInference.src.util.Pair;
import functionalGradient.regressionTree.RegressionProblem;

import util.ConfigRead;
import util.TweetResult;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.REPTree;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.filters.Filter;
import weka.filters.unsupervised.instance.Resample;
import ddInference.src.logic.add_gen.*;

public class RegTree extends REPTree {

    public static final int ARITH_GT    = 1;
    public static final int ARITH_LT    = 1;
    public static final int ARITH_EQ    = 1;
    public static int TREE_DEPTH;
    public static HashMap<String, Integer> var2ID;
    public static RegTree classifier;

    public RegTree(){
        super();
    }


    //method to train a naive bayes classifier
    @Override
    public void buildClassifier(Instances data)throws Exception {
        super.buildClassifier(data);

    }

    //method to classify a new intance using the naive bayes classifyer
    @Override
    public double classifyInstance(Instance newInstance) throws Exception {
        return super.classifyInstance(newInstance);

    }

    /**
     * Valid options are:
     -M <minimum number of instances>
     Set minimum number of instances per leaf (default 2).

     -V <minimum variance for split>
     Set minimum numeric class variance proportion
     of train variance for split (default 1e-3).

     -N <number of folds>
     Number of folds for reduced error pruning (default 3).

     -S <seed>
     Seed for random data shuffling (default 1).

     -P
     No pruning.

     -L
     Maximum tree depth (default -1, no maximum)
     */

    public void setOptions(String[] options) throws Exception {
        //work around to avoid the api print trash in the console
        PrintStream console = System.out;
        System.setOut(new PrintStream(new OutputStream() {
            @Override public void write(int b) throws IOException {}
        }));

        super.setOptions(options);

        //work around to avoid the api print trash in the console
        System.setOut(console);
    }

    public Pair<ArrayList, HashMap> buildRegTree(String dataPath, int treeDepth) throws IOException {
        Pair<ArrayList, HashMap> treeVars = null;
        try {

            RegressionProblem cp = new RegressionProblem(dataPath);
            classifier = new RegTree();
            classifier.setOptions(new String[]{"-M", "1", "-L", Integer.toString(treeDepth), "-V", "-1"});
//            ArrayList<Instances> splittedData = splitData(cp, new double[]{70, 30});
            classifier.buildClassifier(cp.getData());
            treeVars = tree2DDList(classifier.toString());

            /*HashSet leaves = new HashSet();
            ((ADD)_context._context).collectLeaves(((ADD) _context._context)._nRoot, leaves);
            System.out.println(_context._context.toString());
            Evaluation eval = new Evaluation(splittedData.get(0));
            System.out.println(eval.toSummaryString());
            eval.evaluateModel(classifier, splittedData.get(1));
            System.out.println(eval.toSummaryString("\nResults\n======\n", false));
//                System.out.println(i+","+eval.correlationCoefficient());
            */

        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeVars;
    }

    public Pair<ArrayList, HashMap> buildSingleRegTree(String dataPath, int treeDepth) throws IOException {
        Pair<ArrayList, HashMap> treeVars = null;
        try {

            RegressionProblem cp = new RegressionProblem(dataPath);
            classifier = new RegTree();
            classifier.setOptions(new String[]{"-M", "2", "-L", Integer.toString(10), "-V", "0.001"});
            cp.getData().setClassIndex(0);
//            ArrayList<Instances> splittedData = splitData(cp, new double[]{70, 30});
            classifier.buildClassifier(cp.getData());
            Pair<ArrayList, HashMap> resRegTree = tree2DDList(classifier.toString());
            return resRegTree;
            //treeVars = tree2DDList(classifier.toString());

        } catch (Exception e) {
            e.printStackTrace();
        }
        return treeVars;
    }

    public List<TweetResult> evaluateModel(String testDataPath) throws Exception {
        List<TweetResult> tweetWeights = new ArrayList<>();
        RegressionProblem regData = new RegressionProblem(testDataPath);
        regData.getData().setClassIndex(0);
        Evaluation eval = new Evaluation(regData.getData());
        System.out.println(eval.toSummaryString());
        double[] dataPredictions = eval.evaluateModel(classifier, regData.getData());
        double[] trueClasses = regData.getData().attributeToDoubleArray(0);
        System.out.println(eval.toSummaryString("\nResults\n======\n", false));
        int tp = 0, fp = 0, tn = 0, fn = 0;
        //ASSUMPTION: Binary classes equal to 1.0 and -1.0
        for(int i = 0; i < dataPredictions.length; i++){
            tweetWeights.add(new TweetResult(i, dataPredictions[i], "", (int) trueClasses[i]));
            if (dataPredictions[i] == trueClasses[i]){
                if(trueClasses[i] == 1.0)
                    tp++;
                else
                    tn++;
            }else{
                if(trueClasses[i] == 1.0)
                    fn++;
                else
                    fp++;
            }

        }
        System.out.println("TP: " + tp + " out of " + dataPredictions.length);
        System.out.println("FP: " + fp);
        System.out.println("TN: " + tn);
        System.out.println("FN: " + fn);
        return tweetWeights;
    }

    public static ArrayList<Instances> splitData(RegressionProblem cp, double[] percentages) throws Exception {
        Resample filter=new Resample();
        ArrayList<Instances> retData = new ArrayList<>();
        for(double p : percentages) {
            filter.setOptions(new String[]{"-Z", String.valueOf(p), "-no-replacement"});
            filter.setInputFormat(cp.getData());
            retData.add(Filter.useFilter(cp.getData(), filter));
        }
        return retData;
    }

    // input: The Regression Tree through toString() function called on REPTree
    // output: The ArrayList of nodes, the node itself is string, the left and right can be arraylist/string
    // The output is meant to be input to the buildDDFromUnorderedTree function of ADD of DD_Inference (Scott Sanner)
    public static Pair tree2DDList(String classifierStr){
        ArrayList tree = new ArrayList();
        ArrayList left = new ArrayList();
        ArrayList right = new ArrayList();
        /*
        REPTree
        ============

        x = 0 : 0 (2/0) [3/0]
        x = 1
        |   Latitude = 0
        |   |   Longitude = 0 : 0 (4/0) [1/0]
        |   |   Longitude = 1 : 1 (2/0) [1/0]
        |   Latitude = 1 : 1 (2/0) [1/0]

        Size of the tree : 7

        x
        */
        String endLineDelim = "\n", str;
        int op;
        StringTokenizer stkMain = new StringTokenizer(classifierStr, endLineDelim);
        String[] classifierSplits = classifierStr.split(endLineDelim);
        String[] splits;
        int offset = 2;
        double value;
        var2ID = new HashMap<>();
        for(int i = 0; i < offset; i++)
            str = stkMain.nextToken();
        tree = (ArrayList) makeStepTree(stkMain, tree, null, null);
        return new Pair<>(tree, var2ID);
    }

    public static Object makeStepTree(StringTokenizer classifierSplits, ArrayList tree, Object left_tmp, Object right_tmp){
        int op;
        double value;
        String var;
        Object left = null, right = null;
        ArrayList leftArr = null, rightArr = null;
        String[] splits;
        String str = classifierSplits.nextToken();
        StringTokenizer stk;
        boolean flagVar = false;

        if(str.equals(""))
            return null;
        splits = str.split(">");
        op = ARITH_GT;
        if(splits.length <= 1) {
            splits = str.split("<");
            op = ARITH_LT;
        }
        if(splits.length <= 1) {
            splits = str.split("=");
            op = ARITH_EQ;
        }else{
            System.out.println("Wrong!");
        }
        stk = new StringTokenizer(splits[0], "| ");
        var = stk.nextToken();
        if(!var2ID.containsKey(var)) {
            var2ID.put(var, var2ID.size());
            tree.add(var); // add variable
            flagVar = true;
        }
        splits = splits[1].split(":");
        value = Double.valueOf(splits[0]);
//            flagR = false; flagL = false;
        if (value == 0.0) { // left branch
            if (splits.length > 1) {
                value = Double.valueOf(splits[1].split(" ")[1]);
                left = new BigDecimal(value);
                if(classifierSplits.hasMoreElements() && (right_tmp == null && right == null))
                    right = makeStepTree(classifierSplits, new ArrayList(), left, right);
            } else if (classifierSplits.hasMoreElements())
                left = makeStepTree(classifierSplits, new ArrayList(), left, right);
        } else if (value == 1.0) { //right branch
            if (splits.length > 1) {
                value = Double.valueOf(splits[1].split(" ")[1]);
                right = new BigDecimal(value);
                if (classifierSplits.hasMoreElements() && (left_tmp == null && left == null))
                    left = makeStepTree(classifierSplits, new ArrayList(), left, right);
            } else if (classifierSplits.hasMoreElements())
                right = makeStepTree(classifierSplits, new ArrayList(), left, right);
        }
        if(flagVar && (left == null || right == null)){
            if(right == null && classifierSplits.hasMoreElements())
                right = makeStepTree(classifierSplits, new ArrayList(), left, right);
            else if (classifierSplits.hasMoreElements())
                left = makeStepTree(classifierSplits, new ArrayList(), left, right);
        }
        if(left == null){
            return right;
        }else if(right == null)
            return left;
        if(right instanceof BigDecimal) {
            rightArr = new ArrayList();
            rightArr.add(right);
            tree.add(rightArr);
        }else
            tree.add(right);
        if(left instanceof BigDecimal) {
            leftArr = new ArrayList();
            leftArr.add(left);
            tree.add(leftArr);
        }else
            tree.add(left);

        return tree;
    }

    //BufferedReader bufferedReader = new BufferedReader(new FileReader(treeStructurePath));
    public static ArrayList makeStepTreeFromPythonRes(HashMap<Integer, String> inverseFeatureMap, String treeStructFilePath, HashMap<Double, Double> gradUpdates) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(treeStructFilePath));
        String line, nodeName, root = "";
        ArrayList left, right, tmp;
        Object featureNum;
        String[] splits;
        HashMap<String, ArrayList> nodeValues = new HashMap<>();
        int ind = 0;
        double value;

        while((line = bufferedReader.readLine()) != null) {
            ind++;
            splits = line.split(" ");
            nodeName = splits[0];
            if (splits[1].equals("leafNode")) {
                if(!splits[3].equals("[["))
                    value = Double.valueOf(splits[3].split("\\[\\[")[1].split("]]s")[0]);
                else
                    value = Double.valueOf(splits[4].split("]]s")[0]);
                ArrayList al = nodeValues.get(nodeName);
                if(al == null)
                    al = new ArrayList();
                double u = 0;
                if(gradUpdates != null && gradUpdates.get(value) != null)
                    u = gradUpdates.get(value);
                al.add(new BigDecimal(value + u));
            } else {
                featureNum = inverseFeatureMap.get(Integer.valueOf(splits[1].split("X_")[1])+1);//featureNum here starts from zero
                if (!splits[2].equals("<=") || !splits[3].equals("0.5s"))
                    System.out.println("Something is wrong");
                if(nodeValues.containsKey(splits[5]))
                    left = nodeValues.get(splits[5]);
                else
                    left = new ArrayList();
                if(nodeValues.containsKey(splits[7]))
                    right = nodeValues.get(splits[7]);
                else
                    right = new ArrayList();
                nodeValues.put(splits[5], left);
                nodeValues.put(splits[7], right);

                tmp = nodeValues.get(nodeName);
                if(tmp == null)
                    tmp = new ArrayList();
                tmp.add(featureNum);
                tmp.add(right);
                tmp.add(left);
                nodeValues.put(nodeName, tmp);
                if (ind == 1) {
                    root = nodeName;
                }
            }
        }
        return nodeValues.get(root);
    }

}