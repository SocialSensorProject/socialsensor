package functionalGradient;

import ddInference.src.graph.Graph;
import ddInference.src.logic.add.ADD;
import ddInference.src.logic.add_gen.DD;
import ddInference.src.logic.add_gen.FBR;
import machinelearning.LearningProblem;
import util.ConfigRead;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by zahraiman on 2/24/16.
 */
public class TweetToADD {
    public static int maxTreeSize;
    public static int maxLeafNum;
    public static FBR _context;
    public static double prunePrec = 0.8;
    public static LearningProblem learningProblem;
    public final static boolean ALWAYS_FLUSH = false; // Always flush DD caches?
    public final static double FLUSH_PERCENT_MINIMUM = 0.2d; // Won't flush
    public static Runtime RUNTIME = Runtime.getRuntime();

    public TweetToADD(LearningProblem _learningProblem, FBR context) throws IOException {
        ConfigRead configRead = new ConfigRead();
        maxTreeSize = configRead.getMaxTreeSize();
        maxLeafNum = configRead.getMaxLeafNum();
        learningProblem = _learningProblem;
        _context = context;
    }
    /**
     * Compute the multiplication of indicator function of a step t for all query vars
     **/
    public Object getTweetIndicator(String tweet, Map var2ID, Object lastItADD, int classInd, double f0) {
        Object dd = null, ddr = null;
        String[] splits;
        int feat;

        splits = tweet.split(",")[0].split(" ");
        double topicalLabel = Double.valueOf(splits[splits.length - 1]); // Read label first
        splits = tweet.split("[ ,}]");

        //Find the F_m-1 (x_i)
        ArrayList<String> features = new ArrayList<String>();
        for(int i = 1; i < splits.length-1; i+=2){
            features.add(splits[i]);
        }

        //System.out.println(topicalLabel + " => " +  fM_1);

        //Compute the yHat
        double yhat, fM_1;
        if(lastItADD == null) {
            fM_1 = f0;
        }else {
            fM_1 = evaluateSampleInADD(learningProblem.featureMap, features, lastItADD);
        }
        yhat = computeYHat(topicalLabel, fM_1);
        long tid = Long.valueOf(splits[splits.length - 1]);

        ddr = _context.getTidIndicator(tid);
        if(ddr == null) {
            for (int i = 1; i < splits.length - 1; i += 2) {
                int gid = ((Integer) var2ID.get(splits[i])).intValue();
                if (gid == -1)
                    continue;
                dd = _context.getVarNode(gid, 0.0d, 1.0d); // Get a new node
                if (ddr != null)
                    ddr = _context.applyInt(dd, ddr, DD.ARITH_PROD);
                else
                    ddr = dd;
            }
            _context.addTidIndicator(tid, ddr);
        }
//        if(topicalLabel == 1.0)
//            yhat *= (learningProblem.getTotal()[classInd-1] - (learningProblem.getPositives()[classInd-1]))/learningProblem.getPositives()[classInd-1];
        ddr = _context.scalarMultiply(ddr, yhat);
        return ddr;
    }

    public Object convertTweetsToADD(BufferedReader sampleReader, Object lastItADD, int iteration, int classInd, double f0) throws IOException {
        String dotFileName = "tweetsADD";
        String line;
        Object dd = null, pdd = null;
        int trainTweetsNum = 0;
        while ( (line = sampleReader.readLine()) != null) {
            //System.out.println(trainTweetsNum);
            trainTweetsNum++;
            Object tweetADD = getTweetIndicator(line, learningProblem.featureMap, lastItADD, classInd, f0);
            if (dd != null)
                dd = _context.applyInt(tweetADD, dd, DD.ARITH_SUM);
            else
                dd = tweetADD;
            pdd = dd;

            double _prunePrec = prunePrec;
            boolean maxLeafFlag = true;
            while(_context.countExactNodes(dd) > maxTreeSize){//need to prune the tree based on max number of leaves
//                System.out.println("Pruning: " + _prunePrec);
                _context.SetPruneInfo(DD.REPLACE_AVG, _prunePrec);
                if(maxLeafFlag)
                    dd = _context.pruneNodes(dd, maxLeafNum);//-1
                else
                    dd = _context.pruneNodes(dd, -1);
                if(pdd.equals(dd)) {
                    if(!maxLeafFlag)
                        break;
                    maxLeafFlag = false;
                }
                _prunePrec *= 2;
                pdd = dd;
            }
            //flushCaches();

        }
        visualizeGraph(dd, "ADDs/"+dotFileName+"_"+iteration);
        return dd;
    }

    public void visualizeGraph(Object dd, String fileName){
        Graph g = _context.getGraph(dd);
        g.genDotFile(fileName + ".dot");
        //System.out.println(_context.printNode(dd));
        //g.launchViewer(/*width, height*/);
    }
    public double evaluateSampleInADD(Map<String, Integer> var2ID, ArrayList<String> assignments, Object add){

        ArrayList assign = new ArrayList();
        for(Object _id : var2ID.keySet()){
            assign.add(false);
        }

        //assign based on current sample
        for(String _feat : assignments){
//            assign.set(_context._context._alOrder.indexOf(new Integer(var2ID.get(_feat))), true);
            assign.set((Integer) (_context._context)._hmGVarToLevel.get(var2ID.get(_feat)), true);
        }

        double value = _context.evaluate(add, assign);
        return value;
    }

    public double computeYHat(double y, double fM_1){
        double yhat = -2;
        yhat = (2*y) / (1 + Math.exp(-2 * y * fM_1)); // This is the derivation of logistic loss : log(1+exp())
        return yhat;
    }

    public void flushCaches() {
        if (!ALWAYS_FLUSH
                && ((double) RUNTIME.freeMemory() / (double) RUNTIME
                .totalMemory()) > FLUSH_PERCENT_MINIMUM) {
            return; // Still enough free mem to exceed minimum requirements
        }

        _context.flushCaches(false);
    }
}