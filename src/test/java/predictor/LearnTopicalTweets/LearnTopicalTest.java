package predictor.LearnTopicalTweets;

import util.TweetUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by imanz on 10/27/15.
 */
public class LearnTopicalTest {

    public static void makeFakeTweets(){
        int numOfFeatures = 1000;
        int featureNum = 0;
        int trainHashtagNum = 30;
        int testHashtagNum = 24;
        int valHashtagNum = 6;
        int numOfUsers = 100;
        int numOfLocations = 0;
        int numOfMentions = 0;
        int numOfTerms = 0;
        int numOfNoiseHashtags = 0;
        int numOfTweets = 2000;
        int tweetNum = 0;

        List<String> features = new ArrayList<>(numOfFeatures);

        for(int i = 0; i < trainHashtagNum; i++){
            features.add("trainHashtag"+i);
            featureNum++;
        }

        for(int i = 0; i < valHashtagNum; i++){
            features.add("valHashtag"+i);
            featureNum++;
        }

        for(int i = 0; i < testHashtagNum; i++){
            features.add("testHashtag"+i);
            featureNum++;
        }

        features.add("goldenFeature");
        featureNum++;

        for(int i = 0; i < numOfUsers; i++){
            features.add("user"+i);
            featureNum++;
        }

        for(int i = 0; i < 839; i++) {
            if (i % 3 == 0){
                features.add("mention" + i);
                numOfMentions++;
            }if(i % 5 == 0) {
                features.add("noiseHashtag" + i);
                numOfNoiseHashtags++;
            }if(i % 7 == 0) {
                features.add("location" + i);
                numOfLocations++;
            }else {
                features.add("term" + i);
                numOfTerms++;
            }
            featureNum++;
        }

        String tweet = "";
        TweetUtil tweetUtil = new TweetUtil();
        int fromNum, mentionNum, locationNum, noiseHashtagNum, termNum;
        for(int i = 0; i < 14; i++){//topical train tweets
            fromNum = tweetUtil.randInt(0, numOfUsers);
            mentionNum = tweetUtil.randInt(0, numOfMentions);
            locationNum = tweetUtil.randInt(0, numOfLocations);
            termNum = tweetUtil.randInt(0, numOfTerms);
            noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags);

            tweet += "1 " + "from: " + "trainHashtag"+i
        }

    }
}
