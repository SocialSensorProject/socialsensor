package predictor.LearnTopicalTweets;

import org.json.simple.JSONObject;
import util.TweetUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by imanz on 10/27/15.
 */
public class LearnTopicalTest {
    private static String path = "Data/test/Learning/Topics/";
    private static String classname = "naturaldisaster";
    private static SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");

    public static void makeFakeTweets() throws ParseException, IOException, InterruptedException {
        int numOfFeatures = 1000;
        int featureNum = 1;
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

        TweetUtil tweetUtil = new TweetUtil();

        String[] months = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
        String[] days2013 = {"Sun", "Wed", "Wed", "Sat", "Mon", "Thu", "Sat", "Tue", "Fri", "Sun", "Wed", "Fri"};
        String[] days2014 = {"Mon", "Thu", "Thu", "Sun", "Tue", "Wed", "Sun", "Wed", "Sat", "Mon", "Thu", "Sat"};


        tweetUtil.runStringCommand("mkdir " + path + classname + "/");
        tweetUtil.runStringCommand("mkdir " + path + "/featureData/");

        FileWriter fw = new FileWriter(path + classname + "/" +"allHashtag_"+classname+".csv");
        BufferedWriter bw = new BufferedWriter(fw);

        FileWriter fw2 = new FileWriter(path  + "/featureData/" +"featureIndex.csv");
        BufferedWriter bw2 = new BufferedWriter(fw2);

        FileWriter fw3 = new FileWriter(path  + "/featureData/" +"hashtagSet_Date.csv");
        BufferedWriter bw3 = new BufferedWriter(fw3);

        FileWriter fw33 = new FileWriter(path  + "/featureData/" +"hashtagSet_Date2.csv");
        BufferedWriter bw33 = new BufferedWriter(fw33);

        FileWriter fw4 = new FileWriter(path  + "/featureData/" +"hashtagIndex.csv");
        BufferedWriter bw4 = new BufferedWriter(fw4);

        FileWriter fw5 = new FileWriter(path + "out_tweet_hashtag_user_mention_term_time_location_"+1+"_allInnerJoins_parquet_index.csv");
        BufferedWriter bw5 = new BufferedWriter(fw5);

        FileWriter fw6 = new FileWriter(path + "out_tweet_hashtag_user_mention_term_time_location_"+1+"_allInnerJoins_parquet_all.csv");
        BufferedWriter bw6 = new BufferedWriter(fw6);

        List<String> features = new ArrayList<>(numOfFeatures);
        List<String> tweets = new ArrayList<>(numOfTweets);
        List<String> tweetsInds = new ArrayList<>(numOfTweets);
        HashMap<String, Integer> featureInd = new HashMap<String, Integer>(numOfFeatures);

        int index = 0;
        for(int i = 0; i < trainHashtagNum; i++){
            features.add("trainHashtag" + i);
            bw4.write("trainHashtag" + i + "," + featureNum + "\n");
            bw.write("trainHashtag" + i+"\n");
            index = tweetUtil.randInt(0, 9);
            bw3.write("trainHashtag" + i + "," + format.parse(days2013[index] +" "+months[index]+" 20 15:08:01 +0001 2013").getTime()+"\n");
            bw33.write("trainHashtag" + i + "," + format.parse(days2013[index] +" "+months[index]+" 20 15:08:01 +0001 2013").getTime()+","+days2013[index] +" "+months[index] + " 20 15:08:01 +0001 2013" + "\n");
            featureInd.put("trainHashtag" + i, featureNum);
            featureNum++;
        }

        for(int i = 0; i < valHashtagNum; i++){
            features.add("valHashtag" + i);
            bw4.write("valHashtag" + i + "," + featureNum+"\n");
            bw.write("valHashtag" + i+"\n");
            index = tweetUtil.randInt(10, 13);
            if(index < 12) {
                bw3.write("valHashtag" + i + "," + format.parse(days2013[index] + " " + months[index] + " 20 15:08:01 +0001 2013").getTime() + "\n");
                bw33.write("valHashtag" + i + "," + format.parse(days2013[index] +" "+months[index]+" 20 15:08:01 +0001 2013").getTime()+","+days2013[index] +" "+months[index]+" 20 15:08:01 +0001 2013"+"\n");
            }else {
                bw3.write("valHashtag" + i + "," + format.parse(days2014[index - 12] + " " + months[index - 12] + " 20 15:08:01 +0001 2014").getTime() + "\n");
                bw33.write("valHashtag" + i + "," + format.parse(days2014[index - 12] + " " + months[index - 12] + " 20 15:08:01 +0001 2014").getTime()+","+days2014[index - 12] + " " + months[index - 12] + " 20 15:08:01 +0001 2014" + "\n");
            }
            featureInd.put("valHashtag" + i, featureNum);
            featureNum++;
        }

        for(int i = 0; i < testHashtagNum; i++){
            features.add("testHashtag" + i);
            bw4.write("testHashtag" + i + "," + featureNum + "\n");
            bw.write("testHashtag"+i+"\n");
            index = tweetUtil.randInt(2, 11);
            bw3.write("testHashtag" + i + "," + format.parse(days2014[index] +" "+months[index]+" 20 15:08:01 +0001 2014").getTime()+"\n");
            bw33.write("testHashtag" + i + "," + format.parse(days2014[index] +" "+months[index]+" 20 15:08:01 +0001 2014").getTime()+","+days2014[index] +" "+months[index] + " 20 15:08:01 +0001 2014" + "\n");
            featureInd.put("testHashtag" + i, featureNum);
            featureNum++;
        }

        features.add("termgoldenFeature");
        featureInd.put("termgoldenFeature", featureNum);
        featureNum++;


        for(int i = 0; i < numOfUsers; i++){
            features.add("user"+i);
            featureInd.put("user"+i, featureNum);
            featureNum++;
        }

        for(int i = 0; i < 1000-161; i++) {
            if (i % 3 == 0){
                features.add("mentionuser" + numOfMentions);
                numOfMentions++;
            }
            else if (i % 5 == 0) {
                features.add("noiseHashtag" + numOfNoiseHashtags);
                bw4.write("noiseHashtag" +numOfNoiseHashtags + "," + featureNum+"\n");
                bw.write("noiseHashtag" + numOfNoiseHashtags+"\n");
                index = tweetUtil.randInt(0, 23);
                if(index < 12) {
                    bw3.write("noiseHashtag" + numOfNoiseHashtags + "," + format.parse(days2013[index] + " " + months[index] + " 20 15:08:01 +0001 2013").getTime() + "\n");
                    bw33.write("noiseHashtag" + numOfNoiseHashtags + "," + format.parse(days2013[index] + " " + months[index] + " 20 15:08:01 +0001 2013").getTime() + ","+ days2013[index] + " " + months[index] + " 20 15:08:01 +0001 2013" + "\n");
                }else {
                    bw3.write("noiseHashtag" + numOfNoiseHashtags + "," + format.parse(days2014[index - 12] + " " + months[index - 12] + " 20 15:08:01 +0001 2014").getTime() + "\n");
                    bw33.write("noiseHashtag" + numOfNoiseHashtags + "," + format.parse(days2014[index - 12] + " " + months[index - 12] + " 20 15:08:01 +0001 2014").getTime()+","+days2014[index - 12] + " " + months[index - 12] + " 20 15:08:01 +0001 2014" + "\n");
                }
                numOfNoiseHashtags++;
            }else if(i % 7 == 0) {
                features.add("loc" + numOfLocations);
                numOfLocations++;
            }else {
                features.add("term" + numOfTerms);
                numOfTerms++;
            }
            featureInd.put(features.get(featureNum-1), featureNum);
            featureNum++;
        }

        bw.close();
        bw4.close();
        bw3.close();
        bw33.close();
        String tweet = "";


        int fromNum, mentionNum, locationNum, noiseHashtagNum, termNum;
        for(int i = 0; i < 14; i++){//topical train tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "1 " + "from: " + "user"+fromNum + " term: " + "termgoldenFeature" + " hashtag: " + "trainHashtag"+i + " hashtag: " + "trainHashtag"+(i+15);
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " term: " + "term" + termNum;
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " hashtag: " + "noiseHashtag" + noiseHashtagNum;
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " mention: " + "mentionuser" + mentionNum;
            }

            tweet += " location: " + "loc"+locationNum;
            index = tweetUtil.randInt(0, 9);
            tweet += " " + format.parse(days2013[index]+" "+months[index]+" 20 15:08:01 +0001 2013").getTime();
            tweet += " " + tweetNum;
            tweets.add(tweet);
            tweetNum++;
        }

        for(int i = 0; i < 172-14; i++){//topical train tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "0 " + "from: " + "user"+fromNum;
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " term: " + "term" + termNum;
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " hashtag: " + "noiseHashtag" + noiseHashtagNum;
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " mention: " + "mentionuser" + mentionNum;
            }

            tweet += " location: " + "loc"+locationNum;
            index =tweetUtil.randInt(0, 9);
            tweet += " " + format.parse(days2013[index]+" "+months[index]+" 20 15:08:01 +0001 2013").getTime();
            tweet += " " + tweetNum;
            tweets.add(tweet);
            tweetNum++;
        }


        int ind = 0;
        int posNum = 2;
        for(int i = 0; i < posNum; i++){//topical val tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "1 " + "from: " + "user"+fromNum + " term: " + "termgoldenFeature" + " hashtag: " + "valHashtag"+i + " hashtag: " + "valHashtag"+((i+posNum)%valHashtagNum);
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " term: " + "term" + termNum;
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " hashtag: " + "noiseHashtag" + noiseHashtagNum;
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " mention: " + "mentionuser" + mentionNum;
            }

            tweet += " location: " + "loc"+locationNum;
            ind = tweetUtil.randInt(10, 13);
            if(ind < 12)
                tweet += " " + format.parse(days2013[ind]+" "+months[ind]+" 20 15:08:01 +0001 2013").getTime();
            else
                tweet += " " + format.parse(days2014[ind-12]+" "+months[ind-12]+" 20 15:08:01 +0001 2014").getTime();
            tweet += " " + tweetNum;
            tweets.add(tweet);
            tweetNum++;
        }

        for(int i = 0; i < 204-posNum; i++){//topical val tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "0 " + "from: " + "user"+fromNum;
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " term: " + "term" + termNum;
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " hashtag: " + "noiseHashtag" + noiseHashtagNum;
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " mention: " + "mentionuser" + mentionNum;
            }

            tweet += " location: " + "loc"+locationNum;
            ind = tweetUtil.randInt(10, 13);
            if(ind < 12)
                tweet += " " + format.parse(days2013[ind]+" "+months[ind]+" 20 15:08:01 +0001 2013").getTime();
            else
                tweet += " " + format.parse(days2014[ind-12]+" "+months[ind-12]+" 20 15:08:01 +0001 2014").getTime();
            tweet += " " + tweetNum;
            tweets.add(tweet);
            tweetNum++;
        }

        for(int i = 0; i < 24; i++){//topical test tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "1 " + "from: " + "user"+fromNum + " term: " + "termgoldenFeature" + " hashtag: " + "testHashtag"+i + " hashtag: " + "testHashtag"+((i+24)%testHashtagNum);
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " term: " + "term" + termNum;
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " hashtag: " + "noiseHashtag" + noiseHashtagNum;
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " mention: " + "mentionuser" + mentionNum;
            }

            tweet += " location: " + "loc"+locationNum;
            index = tweetUtil.randInt(2, 11);
            tweet += " " + format.parse(days2014[index]+" "+months[index]+" 20 15:08:01 +0001 2014").getTime();
            tweet += " " + tweetNum;
            tweets.add(tweet);
            tweetNum++;
        }

        for(int i = 0; i < 1624-24; i++){//topical test tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "0 " + "from: " + "user"+fromNum;
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " term: " + "term" + termNum;
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " hashtag: " + "noiseHashtag" + noiseHashtagNum;
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " mention: " + "mentionuser" + mentionNum;
            }

            tweet += " location: " + "loc"+locationNum;
            index = tweetUtil.randInt(2, 11);
            tweet += " " + format.parse(days2014[index]+" "+months[index]+" 20 15:08:01 +0001 2014").getTime();
            tweet += " " + tweetNum;
            tweets.add(tweet);
            tweetNum++;
        }

        System.out.println("============================FEATURES================================");
        ind = 1;
        for(String feat: features){
            bw2.write(feat + "," +ind + "\n");
            System.out.println(feat);
            ind++;
        }
        bw2.close();

        String[] splits;
        String tw2;
        List<Integer> featureIndices;
        for(String tw: tweets){
            featureIndices = new ArrayList<>();
            splits = tw.split(" ");
            tw2 = splits[0] + " ";
            Set<Integer> uniqueIndices= new HashSet<>();
            for(int ij = 2; ij < splits.length-2; ij+=2) {
                if(featureInd.get(splits[ij]) == null)
                    System.out.println(splits[ij]);
                uniqueIndices.add(Integer.valueOf(featureInd.get(splits[ij])));

            }
            for(int indice: uniqueIndices)
                featureIndices.add(indice);
            Collections.sort(featureIndices);
            for(int indice:featureIndices)
                tw2 += String.valueOf(indice) + ":1 ";
            tw2 += splits[splits.length-2] + " ";
            tw2 += splits[splits.length-1] + " ";
            tweetsInds.add(tw2);
            bw5.write(tw2 + "\n");
            bw6.write(tw +" " + format.format(new Date(Long.valueOf(splits[splits.length-2])))+"\n");
            bw6.write(tw2 +"\n");
        }
        bw5.close();
        bw6.close();
        //=======================================================================================
        System.out.println("TWeetNum1: " + tweetNum);
        generateTestDataJson(tweets);
        /*tweetNum = 0;
        for(int i = 0; i < 14; i++){//topical train tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "1 " + featureInd.get("user"+fromNum) + ":1" + featureInd.get("termgoldenFeature") + ":1" + featureInd.get("trainHashtag"+i) + ":1" + featureInd.get("trainHashtag"+(i+15)) + ":1";
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("term" + termNum) + ":1";
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " " + featureInd.get("noiseHashtag" + noiseHashtagNum) + ":1";
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("user" + mentionNum) +":1";
            }

            tweet += " " + featureInd.get("loc"+locationNum) +":1";
            tweet += " " + tweetNum;
            tweet += " " + format.parse("Thu "+months[tweetUtil.randInt(0, 10)]+" 20 15:08:01 +0001 2013").getTime();
            tweetsInds.add(tweet);
            bw5.write(tweet + "\n");
            tweetNum++;
        }

        for(int i = 0; i < 172-14; i++){//topical train tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "0 " + featureInd.get("user"+fromNum) + ":1";
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("term" + termNum) + ":1";
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " " + featureInd.get("noiseHashtag" + noiseHashtagNum) + ":1";
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("user" + mentionNum) +":1";
            }

            tweet += " " + featureInd.get("loc"+locationNum) +":1";
            tweet += " " + tweetNum;
            tweet += " " + format.parse("Thu "+months[tweetUtil.randInt(0, 9)]+" 20 15:08:01 +0001 2013").getTime();
            tweetsInds.add(tweet);
            tweetNum++;
            bw5.write(tweet + "\n");
        }

        ind = 0;
        for(int i = 0; i < 5; i++){//topical val tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "1 " + "user"+fromNum + ":1" + "termgoldenFeature" + ":1" + "valHashtag"+i + ":1" + "valHashtag"+((i+5)%valHashtagNum) + ":1";
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("term" + termNum) + ":1";
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " " + featureInd.get("noiseHashtag" + noiseHashtagNum) + ":1";
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("user" + mentionNum) +":1";
            }

            tweet += " " + featureInd.get("loc"+locationNum) +":1";
            tweet += " " + tweetNum;
            ind = tweetUtil.randInt(10, 13);
            if(ind < 12)
                tweet += " " + format.parse("Thu "+months[ind]+" 20 15:08:01 +0001 2013").getTime();
            else
                tweet += " " + format.parse("Thu "+months[ind-12]+" 20 15:08:01 +0001 2014").getTime();
            tweetsInds.add(tweet);
            tweetNum++;
            bw5.write(tweet + "\n");
        }

        for(int i = 0; i < 199; i++){//topical val tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "0 " + featureInd.get("user"+fromNum) + ":1";
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("term" + termNum) + ":1";
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " " + featureInd.get("noiseHashtag" + noiseHashtagNum) + ":1";
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("user" + mentionNum) +":1";
            }

            tweet += " " + featureInd.get("loc"+locationNum) +":1";
            tweet += " " + tweetNum;
            ind = tweetUtil.randInt(10, 13);
            if(ind < 12)
                tweet += " " + format.parse("Thu "+months[ind]+" 20 15:08:01 +0001 2013").getTime();
            else
                tweet += " " + format.parse("Thu "+months[ind-12]+" 20 15:08:01 +0001 2014").getTime();
            tweetsInds.add(tweet);
            tweetNum++;
            bw5.write(tweet + "\n");
        }

        for(int i = 0; i < 25; i++){//topical test tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "1 " + featureInd.get("user"+fromNum) + ":1" + featureInd.get("termgoldenFeature") + ":1" +featureInd.get("testHashtag"+i) + ":1" + featureInd.get("testHashtag"+((i+25)%testHashtagNum)) + ":1";
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("term" + termNum) + ":1";
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " " + featureInd.get("noiseHashtag" + noiseHashtagNum) + ":1";
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("user" + mentionNum) +":1";
            }

            tweet += " " + featureInd.get("loc"+locationNum) +":1";
            tweet += " " + tweetNum;
            tweet += " " + format.parse("Thu "+months[tweetUtil.randInt(2, 11)]+" 20 15:08:01 +0001 2014").getTime();
            tweetsInds.add(tweet);
            tweetNum++;
            bw5.write(tweet + "\n");
        }

        for(int i = 0; i < 1624-25; i++){//topical test tweets
            tweet = "";
            fromNum = tweetUtil.randInt(0, numOfUsers-1);
            locationNum = tweetUtil.randInt(0, numOfLocations-1);

            tweet += "0 " + featureInd.get("user"+fromNum) + ":1";
            for(int j = 0; j < 5; j++) {
                termNum = tweetUtil.randInt(0, numOfTerms-1);
                tweet += " " + featureInd.get("term" + termNum) + ":1";
            }
            for(int j = 0; j < 3; j++) {
                noiseHashtagNum = tweetUtil.randInt(0, numOfNoiseHashtags-1);
                tweet += " " + featureInd.get("noiseHashtag" + noiseHashtagNum) + ":1";
            }
            for(int j = 0; j < 2; j++) {
                mentionNum = tweetUtil.randInt(0, numOfMentions-1);
                tweet += " " + featureInd.get("user" + mentionNum) +":1";
            }

            tweet += " " + featureInd.get("loc"+locationNum) +":1";
            tweet += " " + tweetNum;
            tweet += " " + format.parse("Thu "+months[tweetUtil.randInt(2, 11)]+" 20 15:08:01 +0001 2014").getTime();
            tweetsInds.add(tweet);
            tweetNum++;
            bw5.write(tweet + "\n");
        }
        bw5.close();

        System.out.println("");
        System.out.println("============================TWEETS================================");
        ind = 0;
        for(String tw: tweets){
            System.out.println(tw);
            System.out.println(tweetsInds.get(ind));
            ind++;
        }
        */
    }

    public static void main(String[] args) throws ParseException, IOException, InterruptedException {
        makeFakeTweets();
    }

    public static void generateTestDataJson(List<String> tweets) throws IOException {
        //0 from: user45 term: term168 term: term56 term: term305 term: term200 term: term286 hashtag: noiseHashtag4 hashtag: noiseHashtag89 hashtag: noiseHashtag104 mention: mentionuser271 mention: mentionuser164 location: loc7 1419088021000 465 Sat Dec 20 07:07:01 PST 2014
        //{"screen_name":"avonsafety","created_at":"Fri Feb 28 13:00:00 +0000 2014","id":506187072,"text":"rt @kidrauhlsaussie: justin is a teenageri'm saying it while i can"}
        String tweetStr; int k;
        FileWriter file = new FileWriter(path + "testset_learning.json");
        FileWriter file2 = new FileWriter(path + "user_location_clean.csv");
        HashMap<String, String> userLoc = new HashMap<>();
        int tid = 0;
        JSONObject obj;
        String [] splits;
        String st, time, st2, username = "";
        for(String tweet: tweets){
            k = 0;
            splits = tweet.split(" ");
            k+=2;
            obj = new JSONObject();
            obj.put("screen_name", splits[k]);
            username = splits[k];
            k++;
            st = splits[k];k++;
            tweetStr="";
            time = "";
            while(!st.equals("location:")) {
                switch (st) {
                    case "hashtag:":
                        tweetStr += "#" + splits[k]+ " ";
                        break;
                    case "mention:":
                        tweetStr += "@" + splits[k]+ " ";
                        break;
                    case "term:":
                        tweetStr += splits[k]+ " ";
                        break;
                }
                k++;
                st = splits[k];k++;
            }
            userLoc.put(username, splits[k]);
            //tweetStr += splits[k]; //location
            k++;
            time = format.format(new Date(Long.valueOf(splits[k])));
            k++;
            obj.put("id", Long.valueOf(splits[k]));
            k++;
            obj.put("created_at", time);
            obj.put("text", tweetStr);
            file.write(obj.toJSONString());
            file.write("\n");
            file.flush();

        }
        for(String username1: userLoc.keySet()){
            file2.write(username1 + ","+ userLoc.get(username1)+"\n");
        }
        file.close();
        file2.close();
        //.mode(SaveMode.Overwrite).json("TestSet/testset1_json");
    }
}
