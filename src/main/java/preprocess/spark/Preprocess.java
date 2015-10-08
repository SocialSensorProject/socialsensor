package preprocess.spark;


import com.twitter.Extractor;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.Tuple2;
import preprocess.spark.ConfigRead;
import scala.collection.mutable.Seq;
import scala.runtime.BoxedUnit;
import util.TweetUtil;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Preprocess implements Serializable {



    private static String hdfsPath;
    private static String dataPath; //"TestSet/";
    private static String outputPath; // "TestSet/output_all/";
    private static ConfigRead configRead;
    private static int groupNum = 1;
    private static final double userCountThreshold = 10;
    private static final double featureNum = 1000000;
    private static final double sampleNum = 2000000;
    private static final double featureNumWin = 1000;
    private static final boolean allInnerJoin = false;
    private static final TweetUtil tweetUtil = new TweetUtil();
    private static boolean localRun;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }
    private static Extractor hmExtractor = new Extractor();
    private static int numPart;

    public static void main(String args[]) throws IOException {
        loadConfig();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        dataPath = hdfsPath + configRead.getDataPath(); //configRead.getTestDataPath();
        outputPath = hdfsPath + configRead.getOutputPath(); //configRead.getLocalOutputPath()
        localRun = configRead.isLocal();
        boolean tweetHashtagTime = configRead.isTweetHashtagTime();
        boolean uniqueUserHashtagBirthday = configRead.isUniqueUserHashtagBirthday();
        boolean directedUserNet = configRead.isDirectedUserNet();
        boolean groupedUserMention = configRead.isGroupedUserMention();
        boolean tweetUser = configRead.isTweetUser();
        boolean tweetUserHashtag = configRead.isTweetUserHashtag();
        boolean groupedTweetUserHashtag = configRead.isGroupedTweetUserHashtag();
        boolean tweetMention = configRead.isTweetMention();
        boolean tweetUserMention = configRead.isTweetUserMention();
        boolean groupedTweetHashtagHashtag = configRead.isGroupedTweetHashtagHashtag();
        boolean groupedTweetMentionHashtag = configRead.isGroupedTweetMentionHashtag();
        boolean groupedTweetTermHashtag = configRead.isGroupedTweetTermHashtag();
        SparkConf sparkConfig;
        if(localRun) {
            numPart = 4;
            dataPath = configRead.getTestDataPath();
            outputPath = dataPath;
            sparkConfig = new SparkConf().setAppName("SparkTest").setMaster("local[2]");
        }else {
            sparkConfig = new SparkConf().setAppName("SparkTest");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);

        //readCompleteRaw(sqlContext);
        getFeatures(null, sqlContext);

        DataFrame mainData = null;
        if(localRun) {
            //mainData = sqlContext.read().json(dataPath + "statuses.log.2013-02-01-11.json").coalesce(numPart);
            mainData = sqlContext.read().json(dataPath + "testset1.json").coalesce(numPart);
            //sqlContext.read().parquet("/Users/zahraiman/University/FriendSensor/SPARK/July20/SparkTest/mainData_tweets2014-12.parquet").limit(10000)
        }else if(tweetHashtagTime || uniqueUserHashtagBirthday || directedUserNet || tweetUserHashtag ||tweetUser || groupedTweetHashtagHashtag || groupedTweetMentionHashtag || groupedTweetUserHashtag || groupedTweetTermHashtag || configRead.isTweetTime()) {
            mainData = sqlContext.read().json(dataPath + "tweets2013-2014-v2.0/*.bz2").coalesce(numPart);
        }
        if(configRead.isTweetTime())
            getTweetTime(mainData.select("id", "created_at"), sqlContext);
        if(tweetHashtagTime)
            getTweetHashtagTime(mainData.select("id", "text", "created_at"), sqlContext);
        if(uniqueUserHashtagBirthday)
            getUniqueUsersHashtagsAndBirthdays1(mainData.select("screen_name", "text", "created_at"), sqlContext);
        if(directedUserNet)
            getDirectedUserNetwork(mainData.select("screen_name", "text"), sqlContext);
        if(groupedUserMention)
            getGroupedUserMention(sqlContext);
        if(tweetUser) {
            getTweetUser(mainData.select("id", "screen_name"), sqlContext);
            //DataFrame tweet_user = sqlContext.read().parquet(outputPath + "tweet_user_parquet");
            //tweet_user.distinct();
            //tweet_user.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_user_distinct_parquet");
        }
        if(tweetUserHashtag) {
            getTweetUserHashtag(mainData.select("id", "screen_name", "text"), sqlContext);
            //getTweetUserHashtag(sqlContext.read().json(dataPath + "*.bz2").coalesce(3 * 16).select("id", "screen_name", "text"), sqlContext);
        }
        if(groupedTweetUserHashtag) {
            getGroupedTweetUserHashtag(mainData.select("id", "screen_name", "text"), sqlContext);
            //getTweetMention(sqlContext.read().json(dataPath + "*.bz2").coalesce(3 * 16).select("id", "text"), sqlContext);
        }

        if(groupedTweetHashtagHashtag) {
            getGroupedTweetHashtagHashtag(mainData.select("id", "text"), sqlContext);
            //getTweetMention(sqlContext.read().json(dataPath + "*.bz2").coalesce(3 * 16).select("id", "text"), sqlContext);
        }
        if(tweetUserMention){
            getTweetUserMention(mainData.select("id", "screen_name", "text"), sqlContext);
        }
        if(tweetMention){
            getTweetMention(sqlContext);
        }
        if(groupedTweetMentionHashtag){
            getGroupedTweetMentionHashtag(mainData.select("id", "text"), sqlContext);
        }
        if(groupedTweetTermHashtag){
            getGroupedTweetTermHashtag(mainData.select("id", "text"), sqlContext);
        }

        if(configRead.getHashtagUserFeatures())
            getUserHashtagFeatures(sqlContext);

        if(configRead.getTermFeatures())
            getTermFeatures(sqlContext);

        if(configRead.isWriteHashtagSetBirthday()){
            List<String> hashtagSet = tweetUtil.getGroupHashtagList(3, localRun);
            hashtagSet.addAll(tweetUtil.getGroupHashtagList(4, localRun));
            writeHashtagBirthday(sqlContext, hashtagSet);
        }

        if(configRead.getTestTrainData()) {

            //thresholdMentionHashtagTermFeatures(sqlContext, sparkContext);
            //getGroupedMentionHashtagTermList(sqlContext, sparkContext);
            //getTweetTerm(sqlContext);

            getGroupedMentionHashtagTerm(sqlContext, sparkContext);
            //getTestTrainData(sqlContext);

            /*for (int gNum = 1; gNum <= 4; gNum++) {
                groupNum = gNum;
                System.out.println("==================== Group Num: "+groupNum+"===================");
                System.out.println("==================== ENTERING TWEET TOPICAL===================");
                getTweetTopical(sqlContext);
                getTestTrainData(sqlContext);
                System.out.println("==================== ENTERING TEST TRAIN DATA WITH TOPICAL===================");
                //getTestTrainDataSet(sqlContext);
            }*/

            //writeAsCSV(sqlContext);
        }



        if(configRead.isHashtagBirthdays()){
            getHashtagPairFrequency(sqlContext);
        }

        //getTweetTerm(sqlContext);
        //cleanTerms(sqlContext);
    }



    private static void getGroupedTweetHashtagHashtag(DataFrame tweet_text, SQLContext sqlContext) {
        JavaRDD < Row > t1 = tweet_text.coalesce(numPart).javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                ArrayList<Row> list = new ArrayList<>();
                String hashtag = "";
                for (String word : hmExtractor.extractHashtags(row.get(1).toString())) {
                    hashtag += word.toLowerCase() + ",";
                }
                if (hashtag.endsWith(","))
                    hashtag = hashtag.substring(0, hashtag.length() - 1);
                for (String word : hmExtractor.extractHashtags(row.getString(1))) {
                    list.add(RowFactory.create(row.getLong(0), word.toLowerCase(), hashtag));
                }
                return list;
            }
        });
        StructField[] fields1 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("hashtagGrouped", DataTypes.StringType, true)
        };
        DataFrame t = sqlContext.createDataFrame(t1, new StructType(fields1)).coalesce(numPart);
        t.cache();
        System.out.println("==============FINAL COUNT========= " + t.count());
        t.write().mode(SaveMode.Overwrite).parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet");
    }

    private static void getTweetTopical(SQLContext sqlContext) {
        final List<String> hashtagList = tweetUtil.getGroupHashtagList(groupNum, localRun);
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("topical", DataTypes.IntegerType, true)
        };
        DataFrame df = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").drop("username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                if (v1.getString(1).equals(""))
                    return RowFactory.create(v1.getLong(0), 0);
                List<String> tH = new ArrayList<String>(Arrays.asList((v1.getString(1).split(","))));
                tH.retainAll(hashtagList);
                int numHashtags = tH.size();
                if (numHashtags > 0)
                    return RowFactory.create(v1.getLong(0), 1);
                else
                    return RowFactory.create(v1.getLong(0), 0);
            }
        }), new StructType(fields));
        df.write().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet");
        //DataFrame negativeSamples, positiveSamples;
        // negativeSamples = df.filter(df.col("topical").$eq$eq$eq(0)).coalesce(numPart);
        // positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);

        System.out.println("================== TWEET TOPICAL COUNT: " + df.count() + "========================");
        //System.out.println("================== TWEET TOPICAL POSITIVE COUNT: " + positiveSamples.count() + "========================");
        //System.out.println("================== TWEET TOPICAL NEGATIVE COUNT: " + negativeSamples.count() + "========================");
        //df.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_topical_allTweets_" + groupNum + "_parquet");

        //String name = "tweet_hashtag_user_mention_term_time_parquet";
        //if(allInnerJoin)
        //    name = "tweet_hashtag_user_mention_term_time_allInnerJoins_parquet";
        /*DataFrame df2 = sqlContext.read().parquet(outputPath + "tweet_hashtag_user_mention_term_time_parquet").drop("user").drop("hashtag").drop("term").drop("mentionee").drop("time").drop("username").coalesce(numPart);//.registerTempTable(
        System.out.println("=========== tweet_hashtag_user_mention_term_time COUNT =================== " + df2.count());
        df = df.join(df2, df2.col("tid").equalTo(df.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
        negativeSamples = df.filter(df.col("topical").$eq$eq$eq(0)).coalesce(numPart);
        positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);
        double countVal = sampleNum - positiveSamples.count();
        double countVal2 = negativeSamples.count();
        System.out.println("lLOOOK: "  + countVal + " " + countVal2);
        negativeSamples = negativeSamples.sample(false, (double) (countVal / countVal2));
        df = negativeSamples.unionAll(positiveSamples);

        System.out.println("================== Only tweets with chosen features TWEET TOPICAL COUNT: " + df.count() + "========================");
        System.out.println("================== Only tweets with chosen features TWEET TOPICAL POSITIVE COUNT: " + positiveSamples.count() + "========================");
        System.out.println("================== Only tweets with chosen features TWEET TOPICAL NEGATIVE COUNT: " + negativeSamples.count() + "========================");
        name = "tweet_topical_chosenFeatureTweets_";
        if(allInnerJoin)
            name = "tweet_topical_chosenFeatureTweets_allInnerJoins_";
        df.write().mode(SaveMode.Overwrite).parquet(outputPath + name + groupNum + "_parquet");*/
    }

    private static void getTweetMention(SQLContext sqlContext) {
        output(sqlContext.read().parquet(dataPath + "tweet_user_mention_parquet").drop("username").coalesce(numPart).distinct(), "tweet_mention", false);
    }

    private static void getUniqueUsersHashtagsAndBirthdays1(DataFrame usersHashtagsTime, SQLContext sqlContext){
        JavaRDD<Row> user_hashtags = usersHashtagsTime.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {
                        String username;
                        username = row.get(0).toString();
                        ArrayList<Row> list = new ArrayList<>();
                        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
                        long epochSec = format.parse(row.get(2).toString()).getTime();
                        for (String word : hmExtractor.extractHashtags(row.get(1).toString())) {
                            list.add(RowFactory.create(username.toLowerCase(), word.toLowerCase(), epochSec));
                        }
                        return list;
                    }
                }
        );
        StructField[] fields = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("birthday", DataTypes.LongType, true)
        };
        user_hashtags.cache();
        DataFrame schemaUserHashtags = sqlContext.createDataFrame(user_hashtags, new StructType(fields));
        output(schemaUserHashtags, "user_hashtag_birthday", false);
    }

    private static void getTweetHashtagTime(DataFrame tweetHashtagTime, SQLContext sqlContext){
        JavaRDD<Row> user_hashtags = tweetHashtagTime.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {
                        ArrayList<Row> list = new ArrayList<>();
                        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
                        long epochSec = format.parse(row.get(2).toString()).getTime();
                        for (String word : hmExtractor.extractHashtags(row.get(1).toString())) {
                            list.add(RowFactory.create(row.getLong(0), word.toLowerCase(), epochSec));
                        }
                        return list;
                    }
                }
        );
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        DataFrame schemaUserHashtags = sqlContext.createDataFrame(user_hashtags, new StructType(fields));
        schemaUserHashtags.cache();
        output(schemaUserHashtags, "tweet_hashtag_time", false);
    }

    private static void getTweetTime(DataFrame tweetText, SQLContext sqlContext){
        JavaRDD<Row> user_hashtags = tweetText.javaRDD().map(
                new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
                        long epochSec = format.parse(row.get(1).toString()).getTime();
                        return RowFactory.create(row.get(0), epochSec);
                    }
                }
        );
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        DataFrame schemaUserHashtags = sqlContext.createDataFrame(user_hashtags, new StructType(fields));
        schemaUserHashtags.cache();
        output(schemaUserHashtags, "tweet_time", false);
    }

    private static void getTweetUserHashtag(DataFrame tweetUserHashtag, SQLContext sqlContext){
        JavaRDD<Row> tweet_user_hashtags = tweetUserHashtag.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {
                        ArrayList<Row> list = new ArrayList<>();
                        for (String word : hmExtractor.extractHashtags(row.getString(2))) {
                            list.add(RowFactory.create(row.getLong(0), row.getString(1), word.toLowerCase()));
                        }
                        return list;
                    }
                }
        );
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        DataFrame schemaUserHashtags = sqlContext.createDataFrame(tweet_user_hashtags, new StructType(fields));
        schemaUserHashtags.cache();
        output(schemaUserHashtags, "tweet_user_hashtag", false);
    }

    private static void getTweetUserMention(DataFrame tweetUserMention, SQLContext sqlContext){
        JavaRDD<Row> tweet_mention = tweetUserMention.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {
                        ArrayList<Row> list = new ArrayList<>();
                        for (String word : hmExtractor.extractMentionedScreennames(row.getString(2))) {
                            list.add(RowFactory.create(row.getLong(0), row.getString(1), word.toLowerCase()));
                        }
                        return list;
                    }
                }
        );
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true)
        };
        tweet_mention.cache();
        output(sqlContext.createDataFrame(tweet_mention, new StructType(fields)), "tweet_user_mention", false);
    }

    private static void getTestTrainData(SQLContext sqlContext){
        //Label Hashtag From Mention Term
        DataFrame positiveSamples, negativeSamples, df1, df2;
        //df2 = sqlContext.read().parquet(outputPath + "tweet_hashtag_user_mention_term_time_parquet").drop("user").drop("hashtag").drop("term").drop("mentionee").drop("time").drop("username").coalesce(numPart);//.registerTempTable(
        //System.out.println("=========== tweet_hashtag_user_mention_term_time COUNT =================== " + df2.count());
        //df = df.join(df2, df2.col("tid").equalTo(df.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
        //negativeSamples = df.filter(df.col("topical").$eq$eq$eq(0)).coalesce(numPart);
        //positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);

        DataFrame tweetTopical = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet").coalesce(numPart);
        positiveSamples = tweetTopical.filter(tweetTopical.col("topical").$eq$eq$eq(1)).coalesce(numPart);
        negativeSamples = tweetTopical.filter(tweetTopical.col("topical").$eq$eq$eq(0)).coalesce(numPart);

        double l = positiveSamples.count();
        double l2 = negativeSamples.count();
        System.out.println("=================== POSITIVES/NEGATIVES LEFT ================ " + l + "/" + l2);
        double countVal = sampleNum - l;
        double sampleSize = (double) (countVal / l2);
        System.out.println("LOOOK: " + l + " " + l2);

        DataFrame featureTweetIds = sqlContext.read().parquet(dataPath + "tweet_fromFeature_grouped_parquet").drop("user")
                .coalesce(numPart).unionAll(sqlContext.read().parquet(dataPath + "tweet_termFeature_grouped_parquet")
                        .drop("term").coalesce(numPart)).unionAll(sqlContext.read().
                        parquet(dataPath + "tweet_mentionFeature_grouped_parquet").drop("mentionee").coalesce(numPart))
                .unionAll(sqlContext.read().parquet(dataPath + "tweet_hashtagFeature_grouped_parquet").drop("hashtag")
                        .coalesce(numPart)).unionAll(sqlContext.read().parquet(dataPath + "tweet_locationFeature_grouped_parquet")
                        .drop("C1").coalesce(numPart)).coalesce(numPart).distinct();
        //featureTweetIds.write().parquet(outputPath + "featureTweetIds_parquet");
        System.out.println("================== featureTweetIds COUNT: =========== " + featureTweetIds.count());
        DataFrame negativeTweetIds = featureTweetIds.sample(false, sampleSize).coalesce(numPart);;
        double c = negativeTweetIds.count();
        System.out.println("================== negativeTweetIds COUNT: =========== " + c);
        while(c < sampleNum - l - featureNumWin) {
            featureTweetIds = featureTweetIds.except(negativeTweetIds);
            double tmpCount = featureTweetIds.count();
            System.out.println("================== featureTweetIds COUNT 2: =========== " + tmpCount);
            sampleSize = (sampleNum - l - c) / tmpCount;
            System.out.println("==================SAMPLE SIZE: ============" + sampleSize);
            negativeTweetIds = negativeTweetIds.unionAll(featureTweetIds.sample(false, sampleSize).coalesce(numPart));
            c = negativeTweetIds.count();
            System.out.println("================== negativeTweetIds COUNT2: =========== " + c);
        }

        featureTweetIds = negativeTweetIds.unionAll(positiveSamples.select("tid")).coalesce(numPart).distinct();
        System.out.println("================== featureTweetIds COUNT: =========== " + featureTweetIds.count());

        df2 = sqlContext.read().parquet(dataPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart);
        //df1 = sqlContext.read().parquet(dataPath + "user_location_parquet").coalesce(numPart);
        //df2 = df2.join(df1, df2.col("user").equalTo(df1.col("username")), "left").drop(df1.col("username"));
        //df2.printSchema(); df2.show();
        df1 = featureTweetIds.join(df2, df2.col("tid").equalTo(featureTweetIds.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(dataPath + "tweet_termFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        //df1.write().parquet(outputPath + "tweet_tmp1_parquet");

        //df1 = sqlContext.read().parquet(outputPath + "tweet_tmp1_parquet");
        //System.out.println("================== TMP1 COUNT: =========== " + df1.count());
        df2 = sqlContext.read().parquet(dataPath + "tweet_hashtagFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        //df1.write().parquet(outputPath + "tweet_tmp2_parquet");

        //System.out.println("================== TMP2 COUNT: =========== " + df1.count());
        //df1 = sqlContext.read().parquet(outputPath + "tweet_tmp2_parquet");
        df2 = sqlContext.read().parquet(dataPath + "tweet_mentionFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        //System.out.println("================== COUNT: =========== " + df1.count());
        //df1.cache();

        df2 = sqlContext.read().parquet(dataPath + "tweet_locationFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        df1.printSchema();
        //df1.show(100);
        //System.out.println("========== FEATURES JOINED COUNT ============== " + df1.count());
        //df1.write().parquet(outputPath + "features_joined_out_parquet");


        //System.out.println("========== POSITIVE COUNT ============== " + positiveSamples.count());
        df1 = df1.join(tweetTopical, tweetTopical.col("tid").equalTo(df1.col("tid"))).drop(df1.col("tid")).coalesce(numPart);

        /*positiveSamples = df1.join(positiveSamples, positiveSamples.col("tid").equalTo(df1.col("tid")), "right").drop(positiveSamples.col("tid")).coalesce(numPart);
        negativeSamples = df1.join(negativeSamples, negativeSamples.col("tid").equalTo(df1.col("tid")), "left").drop(negativeSamples.col("tid")).coalesce(numPart);
        negativeSamples = negativeSamples.sample(false, );
        df1 = negativeSamples.unionAll(positiveSamples);*/

        output(df1, "tweet_hashtag_user_mention_term_time_location_"+groupNum+"_allInnerJoins", false);
        df2 = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "FeaturesList_csv").coalesce(numPart);
        df2.printSchema();
        df1 = df1.join(df2, df1.col("hashtag").equalTo(df2.col("C1"))).drop(df2.col("C1")).coalesce(numPart)
                .join(df2, df1.col("user").equalTo(df2.col("C1"))).drop(df2.col("C1")).coalesce(numPart)
                .join(df2, df1.col("mentionee").equalTo(df2.col("C1"))).drop(df2.col("C1")).coalesce(numPart)
                .join(df2, df1.col("term").equalTo(df2.col("C1"))).drop(df2.col("C1")).coalesce(numPart)
                .join(df2, df1.col("location").equalTo(df2.col("C1"))).drop(df2.col("C1")).coalesce(numPart);
        output(df1, "tweet_hashtag_user_mention_term_time_location_strings_"+groupNum+"_allInnerJoins", false);

        //System.out.println("================== Only tweets with chosen features TWEET TOPICAL COUNT: " + df1.count() + "========================");
        //System.out.println("================== Only tweets with chosen features TWEET TOPICAL POSITIVE COUNT: " + positiveSamples.count() + "========================");
        //System.out.println("================== Only tweets with chosen features TWEET TOPICAL NEGATIVE COUNT: " + negativeSamples.count() + "========================");


        System.out.println("================== FINAL TWEET COUNT: =========== " + df1.count());

    }

    private static void getTestTrainDataSet(SQLContext sqlContext) {
        String name = "tweet_hashtag_user_mention_term_time_parquet";
        if(allInnerJoin)
            name = "tweet_hashtag_user_mention_term_time_allInnerJoins_parquet";
        DataFrame df1 = sqlContext.read().parquet(outputPath + name).coalesce(numPart);
        DataFrame df2 = sqlContext.read().parquet(outputPath + "tweet_topical_chosenFeatureTweets_"+groupNum+"_parquet").coalesce(numPart);
        df2.registerTempTable("tweet_topical");
        df2 = df2.join(df1, df2.col("tid").equalTo(df1.col("tid"))).drop(df2.col("tid")).drop(df1.col("tid")).coalesce(numPart);
        df2.printSchema();
        //df2.cache();
        System.out.println("Count: " + df2.count());
        //df2.coalesce(numPart).write().format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save(outputPath + "testTrainData_" + groupNum + "_CSV");

        //System.out.println("================== COUNT: =========== " + df2.count());
        output(df2.coalesce(1), "testTrainData_" + groupNum, false);
        System.out.println("================== COUNT TOPICAL : " + sqlContext.sql("select count(*) from tweet_topical where topical = 1").head().get(0).toString() + "==============================");
    }

    private static void writeAsCSV(SQLContext sqlContext){
        for(int gNum = 1; gNum <= 1; gNum++) {
            DataFrame df = sqlContext.read().parquet("testTrainData_" + gNum + "_parquet").coalesce(numPart);
            df.cache();
            df.coalesce(1).write().format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save("testTrainData_" + groupNum + "_CSV");
        }

    }

    private static void getUniqueUsersHashtagsAndBirthdays(DataFrame usersHashtagsTime, SQLContext sqlContext) {
        JavaRDD<Row> user_hashtags = usersHashtagsTime.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {
                        ArrayList<Row> list = new ArrayList<>();
                        for (String word : hmExtractor.extractHashtags(row.get(0).toString())) {
                            list.add(RowFactory.create(word.toLowerCase()));
                        }
                        return list;
                    }
                }
        );

        StructField[] fields = {
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        DataFrame unique_hashtags = sqlContext.createDataFrame(user_hashtags, new StructType(fields));
        unique_hashtags.distinct().registerTempTable("unique_hashtags");

        output(unique_hashtags, "unique_hashtags", false);

    }
    private static void getTweetUser(DataFrame tweetUser, SQLContext sqlContext){
        JavaRDD<Row> tweet_user = tweetUser.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getLong(0), row.getString(1).toLowerCase());
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true)
        };
        tweet_user.cache();
        output(sqlContext.createDataFrame(tweet_user.distinct(), new StructType(fields)), "tweet_user", false);
    }

    private static void getDirectedUserNetwork(DataFrame userTweets, SQLContext sqlContext) {
        JavaRDD<Row> directed_user = userTweets.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {

                        ArrayList<Row> list = new ArrayList<>();
                        for (String word : hmExtractor.extractMentionedScreennames(row.get(1).toString())) {
                            list.add(RowFactory.create(row.getString(0).toLowerCase(), word.toLowerCase()));
                        }
                        return list;
                    }
                }
        );
        StructField[] fields = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true),
        };
        directed_user.cache();
        DataFrame schemaUserMention = sqlContext.createDataFrame(directed_user.distinct(), new StructType(fields));
        output(schemaUserMention, "user_mention", false);
    }
    private static void output(DataFrame data, String folderName, boolean flag) {
        if(flag)
            data.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + folderName + "_csv");
        data.write().mode(SaveMode.Overwrite).parquet(outputPath + folderName + "_parquet");
    }

    public static void getGroupedUserMention(SQLContext sqlContext){
        DataFrame user_mention = sqlContext.read().parquet(dataPath + "user_mention_parquet").coalesce(3*16);
        JavaPairRDD<String, String> userMentions = user_mention.javaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String str1 = "", str2 = "";
                        if (row.size() > 1) {
                            str1 = row.get(0).toString();
                            str2 = row.get(1).toString();
                        }
                        return new Tuple2<String, String>(str1, str2);
                    }
                });

        JavaPairRDD<String, String> userGroupMentions = userMentions.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "," + s2;
            }
        });

        JavaRDD<Row> usermentionRdd = userGroupMentions.map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return RowFactory.create(stringStringTuple2._1(), stringStringTuple2._2());
            }
        });

        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true),
        };
        DataFrame user_mentione_grouped = sqlContext.createDataFrame(usermentionRdd, new StructType(fields1));

        user_mentione_grouped.cache();
        user_mentione_grouped.write().mode(SaveMode.Overwrite).parquet(dataPath + "user_mentione_grouped_parquet");

    }

    public static void getGroupedTweetUserHashtag(DataFrame tweet_user_text, SQLContext sqlContext){
        System.out.println("************************** " + dataPath + "tweet_user_hashtag_grouped_parquet");

        //JavaRDD<Row> t1 = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart).javaRDD().mapToPair(
        JavaRDD<Row> t1 = tweet_user_text.coalesce(numPart).javaRDD().map(
                new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        String hashtag = "";
                        for (String word : hmExtractor.extractHashtags(row.get(2).toString())) {
                            hashtag += word.toLowerCase() + ",";
                        }
                        if (hashtag.endsWith(","))
                            hashtag = hashtag.substring(0, hashtag.length() - 1);
                        return RowFactory.create(row.getLong(0), row.getString(1), hashtag);
                    }
                }
        );
        StructField[] fields1 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        DataFrame t = sqlContext.createDataFrame(t1, new StructType(fields1)).coalesce(numPart);
        t.cache();
        System.out.println("==========FINAL COUNT============= " + t.count());
        t.write().mode(SaveMode.Overwrite).parquet(dataPath + "tweet_user_hashtag_grouped_parquet");
    }

    public static void getGroupedTweetMentionHashtag(DataFrame tweet_text, SQLContext sqlContext){
        System.out.println("************************** " + dataPath + "tweet_hashtag_time_parquet");
        JavaRDD<Row> t1 = tweet_text.coalesce(numPart).javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                ArrayList<Row> list = new ArrayList<>();
                String hashtag = "";
                for (String word : hmExtractor.extractHashtags(row.get(1).toString())) {
                    hashtag += word.toLowerCase() + ",";
                }
                if (hashtag.endsWith(","))
                    hashtag = hashtag.substring(0, hashtag.length() - 1);
                for (String word : hmExtractor.extractMentionedScreennames(row.getString(1))) {
                    list.add(RowFactory.create(row.getLong(0), word.toLowerCase(), hashtag));
                }
                return list;
            }
        });
        StructField[] fields1 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        DataFrame t = sqlContext.createDataFrame(t1, new StructType(fields1)).coalesce(numPart);
        t.cache();
        //System.out.println("==========FINAL COUNT============= " + t.count());
        t.write().mode(SaveMode.Overwrite).parquet(dataPath + "tweet_mention_hashtag_grouped_parquet");
        System.out.println("==========FINAL COUNT============= " + t.count());
    }

    public static void getGroupedMentionHashtagTerm(SQLContext sqlContext, JavaSparkContext sc){
        //final List<String> hashtagList = getGroupHashtagList(groupNum);
        System.out.println("************************** " + dataPath + "tweet_mention_parquet");
        StructField[] fields2 = {
                DataTypes.createStructField("id", DataTypes.LongType, true)
        };
        StructField[] fieldsMention = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true)
        };
        StructField[] fieldsFrom = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("user", DataTypes.StringType, true)
        };
        StructField[] fieldsHashtag = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        StructField[] fieldsTerm = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("term", DataTypes.StringType, true)
        };
        StructField[] fieldsLocation = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("location", DataTypes.StringType, true)
        };
        StructField[] tmp = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("id", DataTypes.LongType, true)
        };
        StructField[] tmp2 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.DoubleType, true)
        };
        long ind = 1;DataFrame df2;
        final long ind1 = ind;
        DataFrame featuresList;
        final int fromThreshold= 159, mentionThreshold= 159, hashtagThreshold= 50, termThreshold= 159;
        DataFrame fromNumberMap;
        DataFrame df1;
        df2 = sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);
        df2.show();
        df2.select(df2.col("tid").equalTo(785573287l)).show();
        df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);
        df2.show();
        df1 = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(1), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getDouble(1) >= fromThreshold;
            }
        }), new StructType(tmp2));

        fromNumberMap = sqlContext.createDataFrame(df1.drop("count").javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind1);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        featuresList = fromNumberMap;
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("username").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("username")).coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getLong(0), String.valueOf(v1.getLong(1)));
            }
        }), new StructType(fieldsFrom));
        output(df2, "tweet_fromFeature_grouped", false);
        df2.show();
        System.out.println("==================DOUBLE CHECK SIZES=================: " + df2.count());
        System.out.println("================== IND VALUE AFTER FROM_FEATURE=================: " + ind);


        final long ind2 = ind;
        df2 = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").distinct().coalesce(numPart);
        df2.show();
        df1 = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(1), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getDouble(1) >= termThreshold;
            }
        }), new StructType(tmp2));
        fromNumberMap = sqlContext.createDataFrame(df1.drop("count").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind2);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        featuresList = featuresList.unionAll(fromNumberMap).coalesce(numPart);
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("term").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("term")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), String.valueOf(row.getLong(1)));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String aDouble, String aDouble2) throws Exception {
                return aDouble + " " + aDouble2;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fieldsTerm)).coalesce(numPart);
        output(df2, "tweet_termFeature_grouped", false);
        //System.out.println("==========FINAL TERM COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER TERM_FEATURE=================: " + ind);

        final long ind3 = ind;
        df2 = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart);//.registerTempTable("tweetMention");
        df1 = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(1), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getDouble(1) >= hashtagThreshold;
            }
        }), new StructType(tmp2));
        fromNumberMap = sqlContext.createDataFrame(df1.drop("count").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind3);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        featuresList = featuresList.unionAll(fromNumberMap).coalesce(numPart);
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("hashtag").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("hashtag")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), String.valueOf(row.getLong(1)));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String aDouble, String aDouble2) throws Exception {
                return aDouble + " " + aDouble2;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fieldsHashtag)).coalesce(numPart);
        output(df2, "tweet_hashtagFeature_grouped", false);

        final long ind4 = ind;
        //System.out.println("==========FINAL HASHTAG COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER HASHTAG_FEATURE=================: " + ind);

        df2 = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);//.registerTempTable("tweetMention");
        df1 = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(1), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getDouble(1) >= mentionThreshold;
            }
        }), new StructType(tmp2));
        fromNumberMap = sqlContext.createDataFrame(df1.drop("count").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind4);
            }
        }), new StructType(tmp)).coalesce(numPart);
        double mentionCount = fromNumberMap.count();
        ind += mentionCount;
        featuresList = featuresList.unionAll(fromNumberMap).coalesce(numPart);
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("mentionee").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("mentionee")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), String.valueOf(row.getLong(1)));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String aDouble, String aDouble2) throws Exception {
                return aDouble + " " + aDouble2;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fieldsMention)).coalesce(numPart);
        output(df2, "tweet_mentionFeature_grouped", false);
        System.out.println("==========FINAL Mention COUNT============= " + mentionCount);

        final long ind5 = ind;
        df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);
        df1 = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "user_location_clean.csv").coalesce(numPart);
        df1.printSchema();
        df1.show();
        df2 = df2.join(df1, df2.col("username").equalTo(df1.col("C0"))).drop(df2.col("username")).drop(df1.col("C0"));//tid, location
        df1 = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(1), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(tmp2));
        fromNumberMap = sqlContext.createDataFrame(df1.drop("count").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind5);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        featuresList = featuresList.unionAll(fromNumberMap).coalesce(numPart);
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("C1").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("C1")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), String.valueOf(row.getLong(1)));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String aDouble, String aDouble2) throws Exception {
                return aDouble + " " + aDouble2;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fieldsLocation)).coalesce(numPart);
        output(df2, "tweet_locationFeature_grouped", false);
        //System.out.println("==========FINAL TERM COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER Location_FEATURE=================: " + ind);


        System.out.println("==========FINAL Feature COUNT============= " + (ind - 1));
        featuresList.coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "FeaturesList_csv");

        /*df1 = sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart);
        df2 = sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "outer").drop(df2.col("tid")).coalesce(numPart);
        df2 = sqlContext.read().parquet(outputPath + "tweet_hashtagFeature_grouped_parquet").coalesce(numPart);
        System.out.println("==========FINAL HASHTAG : " + df1.drop("tid").distinct().count() );
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "outer").drop(df2.col("tid")).coalesce(numPart);
        output(df1, "tweet_from_term_hashtag", false);
        df2 = sqlContext.read().parquet(outputPath + "tweet_mentionFeature_grouped_parquet").coalesce(numPart);
        System.out.println("==========FINAL MENTION : " + df1.count() );
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "outer").drop(df2.col("tid")).coalesce(numPart);
        output(df1, "tweet_from_term_hashtag_mention", false);
        df2 = sqlContext.read().parquet(outputPath + "tweet_locationFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "outer").drop(df2.col("tid")).coalesce(numPart);
        output(df1, "tweet_from_term_hashtag_mention_location", false);*/
    }

    public static void getGroupedTweetTermHashtag(DataFrame tweet_text, SQLContext sqlContext){
        StructField[] fields1 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("term", DataTypes.StringType, true),
                //DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        final String emo_regex2 = "([\\u20a0-\\u32ff\\ud83c\\udC00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee])";//"\\p{InEmoticons}";
        System.out.println("************************** " + dataPath + "tweet_term_time_parquet");
        DataFrame t1 = sqlContext.createDataFrame(tweet_text.coalesce(numPart).javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                long id = row.getLong(0);
                ArrayList<Row> list = new ArrayList<>();
                /*String hashtag = "";
                for (String word : hmExtractor.extractHashtags(row.getString(1))) {
                    hashtag += word.toLowerCase() + ",";
                }
                if (hashtag.endsWith(","))
                    hashtag = hashtag.substring(0, hashtag.length() - 1);*/
                Matcher matcher = Pattern.compile(emo_regex2).matcher(row.getString(1));
                while(matcher.find())
                    list.add(RowFactory.create(id, matcher.group().toLowerCase()));//, hashtag));
                String text = matcher.replaceAll("").trim();
                StringTokenizer stok = new StringTokenizer(text, "\'\"?, ;.:!()-*«“|><`~$^&[]\\}{=”•’…‘！′：+´");
                String str=""; boolean write = true, isUrl = false, containHttp = false;
                while(stok.hasMoreTokens()){
                    write = true;
                    str = stok.nextToken();
                    while(containHttp || str.contains("@") || str.contains("#") || str.contains("http")){//"#that#this@guy did "
                        if(containHttp){
                            while (str.contains("/")) {
                                if (!stok.hasMoreTokens()) {
                                    write = false;
                                    break;
                                }
                                str = stok.nextToken();
                            }
                            containHttp = false;
                        }
                        isUrl = str.startsWith("http");
                        if(!isUrl) {
                            if(str.contains("http")){
                                containHttp = true;
                                if(str.split("http")[0].length() > 0){
                                    str = str.split("http")[0];
                                    if (str.length() == 0)
                                        write = false;
                                }else
                                    write = false;
                            }if(str.contains("@") || str.contains("#")) {
                                if (str.split("[@#]").length > 0) {
                                    str = str.split("[@#]")[0];
                                    if (str.length() == 0)
                                        write = false;
                                } else
                                    write = false;
                            }
                            break;
                        }
                        if(!stok.hasMoreTokens()) {
                            write = false;
                            break;
                        }else {
                            str = stok.nextToken();
                            if (isUrl) {
                                while (str.contains("/")) {
                                    if (!stok.hasMoreTokens()) {
                                        write = false;
                                        break;
                                    }
                                    str = stok.nextToken();
                                }
                            }
                        }
                    }
                    if(write) {
                        if(str.contains("/")) {
                            for(String st: str.split("/")) {
                                list.add(RowFactory.create(id, st.toLowerCase()));//, hashtag));
                            }
                        }else {
                            list.add(RowFactory.create(id, str.toLowerCase()));//, hashtag));
                        }
                    }
                }
                return list;
            }
        }), new StructType(fields1)).coalesce(numPart);

        //sqlContext.createDataFrame(t1, new StructType(fields1)).coalesce(numPart).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(dataPath + "tweet_term_hashtag_grouped_csv");
        //System.out.println("========= TWEET TERM COUNT: "+t1.count()+"===================");
        t1.cache();
        t1.distinct().coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(dataPath + "tweet_term_parquet");
        //System.out.println("==========FINAL COUNT============= " + t.count());
    }


    public static void getUserHashtagFeatures(SQLContext sqlContext){

        sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart).registerTempTable("userTweet");
        DataFrame df1 = sqlContext.sql("SELECT mentionee, count(tid) AS tweetCount from userTweet GROUP BY mentionee").coalesce(numPart);
        df1.sort(df1.col("tweetCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "mention_tweetCount_parquet");

        /*
        sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart).registerTempTable("userTweet");
        df1 = sqlContext.sql("SELECT username, count(tid) AS tweetCount from userTweet GROUP BY username").coalesce(numPart);
        df1.sort(df1.col("tweetCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "user_tweetCount_parquet");

        sqlContext.read().parquet(dataPath + "user_hashtag_birthday_parquet").drop("birthday").coalesce(numPart).distinct().registerTempTable("userHashtag");
        df1 = sqlContext.sql("SELECT username, count(hashtag) AS hashtagCount from userHashtag GROUP BY username").coalesce(numPart);
        df1.sort(df1.col("hashtagCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "user_hashtagCount_parquet");
        df1 = sqlContext.sql("SELECT hashtag, count(username) AS userCount from userHashtag GROUP BY hashtag").coalesce(numPart);
        System.out.println("==========FINAL COUNT hashtag-user ============= " + df1.count());
        df1.sort(df1.col("userCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "hashtag_userCount_parquet");

        sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart).distinct().registerTempTable("tweetHashtag");
        df1 = sqlContext.sql("SELECT hashtag, count(tid) AS tweetCount from tweetHashtag GROUP BY hashtag").coalesce(numPart);
        System.out.println("==========FINAL COUNT hashtag-tweet ============= " + df1.count());
        df1.sort(df1.col("tweetCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "hashtag_tweetCount_parquet");
        */
        //df2.sort(df2.col("tweetCount").desc()).coalesce(1).write().format("com.databricks.spark.csv").save(dataPath + "hashtag_tweetCount_csv");
    }

    public static void getTermFeatures(SQLContext sqlContext){
        //.coalesce(numPart).registerTempTable("tweet_term_hashtag");
        StructField[] fields1 = {
                DataTypes.createStructField("term", DataTypes.StringType, true),
                DataTypes.createStructField("tweetCount", DataTypes.DoubleType, true)
        };
        DataFrame df1 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                return new Tuple2<String, Double>(row.getString(1), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fields1)).coalesce(numPart);
        System.out.println("==========FINAL COUNT Term============= " + df1.count());
        ///DataFrame df1 = sqlContext.sql("SELECT term, count(tid) AS tweetCount from tweet_term_hashtag GROUP BY term").coalesce(numPart);
        //df1.cache();
        df1.sort(df1.col("tweetCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(dataPath + "term_tweetCount_parquet");
        //df1.sort(df1.col("tweetCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "term_tweetCount_parquet");
    }

    public static void readCompleteRaw(SQLContext sqlContext){
        //DataFrame mainData = sqlContext.read().json(dataPath + "tweets2013-2014-v3.0/tweets2013-02.txt.bz2").coalesce(numPart);
        //tweets2014-12-v2.txt
        //DataFrame mainData = sqlContext.read().json(dataPath + "tweets2013-2014-v3.0/*.bz2").select("screen_name",
        //        "followers_count", "listed_count", "favorite_count", "statuses_count", "friends_count", "user_location", "user_timezone").coalesce(numPart);

        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.StringType, true),
                DataTypes.createStructField("place_country_code", DataTypes.StringType, true),
                DataTypes.createStructField("place_full_name", DataTypes.StringType, true),
                DataTypes.createStructField("tweet_geo_lat", DataTypes.StringType, true),
                DataTypes.createStructField("tweet_geo_lng", DataTypes.StringType, true),
                DataTypes.createStructField("tweet_favorite_count", DataTypes.StringType, true),
                DataTypes.createStructField("retweet_count", DataTypes.StringType, true),
                DataTypes.createStructField("created_at", DataTypes.StringType, true)
        };
        DataFrame mainData = sqlContext.createDataFrame(sqlContext.read().json(dataPath + "tweets2014-10.txt.bz2").select("id",
                "place_country_code", "place_full_name", "tweet_geo_lat", "tweet_geo_lng", "tweet_favorite_count", "retweet_count", "created_at").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.get(0) == null || row.get(1) == null || row.get(2) == null||row.get(3) == null||row.get(4) == null||row.get(5) == null
                ||row.get(6) == null||row.get(7) == null)
                    return RowFactory.create("-1", "", "", "", "", "", "", "");
                /*double favCount, retweetCount;
                if(row.get(5).toString().equals("null") || row.get(5).toString().equals(""))
                    favCount = -1;
                else
                    favCount = Double.valueOf(row.get(5).toString()).doubleValue();
                if(row.get(6).toString().equals("null") || row.get(6).toString().equals(""))
                    retweetCount = -1;
                else
                    retweetCount = Double.valueOf(row.get(6).toString());*/
                return RowFactory.create(row.get(0).toString(), row.get(1).toString(), row.get(2).toString(), row.get(3).toString()
                        ,row.get(4).toString(), row.get(5).toString(), row.get(6).toString(), row.get(7).toString());
            }
        }), new StructType(fields));
        mainData.printSchema();

        mainData.write().mode(SaveMode.Overwrite).parquet(dataPath + "tweet_features1_parquet");
        System.out.println("=================== ID : " + mainData.select("id").count() + "====================");
        //DataFrame mainData = sqlContext.read().json(dataPath + "tweets2014-12-v2.txt.bz2").coalesce(numPart);


        //getFeatures(mainData, sqlContext);
        getTweetFeatures(sqlContext);
    }

    public static void getFeatures(DataFrame data, SQLContext sqlContext){
        //{"tweet_favorite_count":"0","tweet_geo_lat":null,"tweet_geo_lng":null,"user_location":"","statuses_count":"1888",
        // "place_country_code":null,"id":461520349717209088,"user_timezone":null,"friends_count":"118","place_full_name":null,
        // "retweet_count":"0","created_at":"Wed Apr 30 15:00:00 +0000 2014","screen_name":"makaylaagarcia","favorite_count":"1052",
        // "followers_count":"122","listed_count":"0"}
        StructField[] userFeatureCounts = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("featureCount", DataTypes.LongType, true)
        };
        if(data == null)
            data = sqlContext.read().parquet(dataPath + "user_features_parquet").distinct().coalesce(numPart);
        //System.out.println("===============Count: " + data.count() + "==="+data.select("screen_name").distinct().count()+ "==================");

        String[] features = {"followers_count","listed_count", "favorite_count", "statuses_count", "friends_count"};
        final String[] locationFeatures = {"user_location", "user_timezone"};
        StructField[] userLocationField = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("user_location", DataTypes.StringType, true)
                //,DataTypes.createStructField("user_timezone", DataTypes.StringType, true)
        };

        for(String featureName : features) {
            DataFrame df1 = sqlContext.createDataFrame(data.select("screen_name", featureName).coalesce(numPart).distinct().javaRDD().mapToPair(new PairFunction<Row, String, Long>() {
                @Override
                public Tuple2<String, Long> call(Row row) throws Exception {
                    if (row.get(0) == null || row.get(1) == null || row.getString(1).equals("null") || row.getString(0).equals("null") || row.getString(1).equals("") || row.getString(0).equals(""))
                        return new Tuple2<String, Long>("-1deletethis", 0l);
                    else
                        return new Tuple2<String, Long>(row.getString(0).toLowerCase(), Long.valueOf(row.getString(1)));
                }
            }).reduceByKey(new Function2<Long, Long, Long>() {
                @Override
                public Long call(Long aLong, Long aLong2) throws Exception {
                    if(aLong > aLong2)
                        return aLong;
                    else
                        return aLong2;
                }
            }).map(new Function<Tuple2<String, Long>, Row>() {
                @Override
                public Row call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                    return RowFactory.create(stringLongTuple2._1(), stringLongTuple2._2());
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return !v1.get(0).toString().equals("-1deletethis");
                }
            }), new StructType(userFeatureCounts)).coalesce(numPart);
            //System.out.println("==========FINAL "+featureName+"============= " + df1.count());
            df1.sort(df1.col("featureCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(dataPath + "user_" + featureName + "_parquet");
        }

        DataFrame df1 = sqlContext.createDataFrame(data.select("screen_name", locationFeatures[0]).coalesce(numPart).distinct().javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0).toLowerCase(), v1.getString(1));
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                if ((v1.get(1) == null || v1.getString(1).equals("") || v1.getString(1).equals("null")))// && (v1.get(2) == null || v1.getString(2).equals("") || v1.getString(2).equals("null")))
                    return false;
                else
                    return true;
            }
        }), new StructType(userLocationField)).coalesce(numPart).distinct().coalesce(numPart);
        System.out.println("==========FINAL userLocationField ============= " + df1.count());
        df1.coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(dataPath + "user_location_parquet");
    }

    public static void getTweetFeatures(SQLContext sqlContext){
        String[] features = {"tweet_favorite_count", "retweet_count"};
        String[] tweetFeatures = {"place_country_code","place_full_name", "tweet_geo_lat", "tweet_geo_lng"};
        DataFrame data = sqlContext.read().parquet(dataPath + "tweet_features_parquet").distinct().coalesce(numPart);
        StructField[] tweetFeatureCounts = {
                DataTypes.createStructField("tid", DataTypes.StringType, true),
                DataTypes.createStructField("featureCount", DataTypes.DoubleType, true)
        };

        for(String featureName : features) {
            DataFrame df1 = sqlContext.createDataFrame(data.select("id", featureName).coalesce(numPart).distinct().javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Row row) throws Exception {
                    if (row.get(1) == null || row.getString(1).equals("null") || row.getString(1).equals(""))
                        return new Tuple2<String, Double>(row.getString(0), 0.0);
                    else
                        return new Tuple2<String, Double>(row.getString(0), Double.valueOf(row.getString(1)));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            }).map(new Function<Tuple2<String, Double>, Row>() {
                @Override
                public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                }
            }), new StructType(tweetFeatureCounts)).coalesce(numPart);
            //System.out.println("==========FINAL "+featureName+"============= " + df1.count());
            df1.sort(df1.col("featureCount").desc()).coalesce(1).write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_" + featureName + "_parquet");
        }

        StructField[] tweetLocationField = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("place_country_code", DataTypes.StringType, true),
                DataTypes.createStructField("place_full_name", DataTypes.StringType, true),
                DataTypes.createStructField("tweet_geo_lat", DataTypes.StringType, true),
                DataTypes.createStructField("tweet_geo_lng", DataTypes.StringType, true)
        };
        DataFrame df1 = sqlContext.createDataFrame(data.select("id", tweetFeatures[0], tweetFeatures[1],tweetFeatures[2], tweetFeatures[3]).coalesce(numPart).distinct().javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                if((v1.get(1) == null || v1.getString(1).equals("") || v1.getString(1).equals("null")) && (v1.get(2) == null || v1.getString(2).equals("") || v1.getString(2).equals("null"))
                        && (v1.get(1) == null || v1.getString(3).equals("") || v1.getString(3).equals("null")) && (v1.get(4) == null || v1.getString(4).equals("") || v1.getString(2).equals("null")))
                    return false;
                else
                    return true;
            }
        }), new StructType(tweetLocationField)).coalesce(numPart);
        System.out.println("==========FINAL userLocationField ============= " + df1.count());
        df1.coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_location_parquet");

    }

    public static void cleanTerms(SQLContext sqlContext){
        StructField[] fields1 = {
                DataTypes.createStructField("term1", DataTypes.StringType, true),
                DataTypes.createStructField("freq", DataTypes.DoubleType, true)
        };
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("term", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        DataFrame df1 = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "cleanTerms").javaRDD(), new StructType(fields1)).drop("freq");
        DataFrame df2 = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet");
        //System.out.println("============== FINAL COUNT TERM NOT REMOVED ============== " + df2.count());10267957382

        DataFrame df3 = df2.join(df1, df1.col("term1").equalTo(df2.col("term"))).drop("term1");
        df3.printSchema();
        df3.write().parquet(dataPath + "tweet_cleanTerms_hashtag_grouped_parquet");
        System.out.println("============== FINAL COUNT TERM REMOVED ============== " + df3.count());
        /*
        List<String> cleanTerms = new ArrayList<String>();
        for(Row r :sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "cleanTerms2").javaRDD(), new StructType(fields1)).drop("freq").collectAsList())
            cleanTerms.add(r.getString(0));
        final List<String> terms = cleanTerms;
        DataFrame df2 = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "tweet_term_hashtag_grouped_parquet").javaRDD(), new StructType(fields));//.registerTempTable("tweet_freq");
        df2 = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "tweet_term_hashtag_grouped_parquet").javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        if (terms.contains(row.getString(1)))
                            return RowFactory.create(row.getLong(0), row.getString(1), row.getString(2));
                        else
                            return null;
                    }
                }), new StructType(fields));
        System.out.println("Count: " + df2.count());
         */
    }

    public static void getTweetTerm(SQLContext sqlContext){
        DataFrame df = sqlContext.read().parquet(outputPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").distinct().coalesce(numPart);
        System.out.println("COUNT: " + df.count());
        //df.cache();
        df.coalesce(40).write().parquet(dataPath + "tweet_term_parquet");
    }



    public static void getGroupedMentionHashtagTermList(SQLContext sqlContext, JavaSparkContext sc){
        //final List<String> hashtagList = getGroupHashtagList(groupNum);
        System.out.println("************************** " + dataPath + "tweet_mention_parquet");
        StructField[] tmp = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("id", DataTypes.LongType, true)
        };
        StructField[] tmp2 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.DoubleType, true)
        };
        long ind = 1;DataFrame df2;
        final long ind1 = ind;
        //ind += sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        //ind += sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        DataFrame fromNumberMap;
        df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        fromNumberMap = sqlContext.createDataFrame(df2.drop("tid").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind1);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("username").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(1) + "," + row.getLong(2), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(tmp2));
        df2.show(100);
        output(df2, "fromFeature_count_grouped", false);
        System.out.println("================== IND VALUE AFTER FROM_FEATURE=================: " + ind);

        final long ind2 = ind;
        df2 = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);//.registerTempTable("tweetMention");
        fromNumberMap = sqlContext.createDataFrame(df2.drop("tid").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind2);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("term").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).distinct().coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, term, id
                return new Tuple2<String, Double>(row.getString(1) + "," + row.getLong(2), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(tmp2));
        output(df2, "termFeature_count_grouped", false);
        System.out.println("==========FINAL TERM COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER TERM_FEATURE=================: " + ind);

        final long ind3 = ind;
        df2 = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart);//.registerTempTable("tweetMention");
        fromNumberMap = sqlContext.createDataFrame(df2.drop("tid").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind3);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("hashtag").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).distinct().coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, term, id
                return new Tuple2<String, Double>(row.getString(1) + "," + row.getLong(2), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(tmp2));
        output(df2, "hashtagFeature_count_grouped", false);
        System.out.println("==========FINAL HASHTAG COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER HASHTAG_FEATURE=================: " + ind);

        final long ind4 = ind;
        df2 = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);//.registerTempTable("tweetMention");
        fromNumberMap = sqlContext.createDataFrame(df2.drop("tid").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind4);
            }
        }), new StructType(tmp)).coalesce(numPart);
        ind += fromNumberMap.count();
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("mentionee").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).distinct().coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, term, id
                return new Tuple2<String, Double>(row.getString(1) + "," + row.getLong(2), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(tmp2));
        output(df2, "tweet_mentionFeature_grouped", false);
        System.out.println("==========FINAL Mention COUNT============= " + df2.count());
        System.out.println("==========FINAL Feature COUNT============= " + (ind - 1));


        df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        df2.unionAll(sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart));

        fromNumberMap = sqlContext.createDataFrame(df2.drop("tid").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind1);
            }
        }), new StructType(tmp)).coalesce(numPart);
        System.out.println("============ ALL USER's COUNT:"+fromNumberMap.count()+"=====================");

        output(df2, "fromMentionList", false);

        df2 = sqlContext.read().parquet(dataPath + "testTrainData_1_parquet").coalesce(numPart);
        df2.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(dataPath + "testTrainData_1_csv");
    }



    public static void thresholdMentionHashtagTermFeatures(SQLContext sqlContext, JavaSparkContext sc){
        //final List<String> hashtagList = getGroupHashtagList(groupNum);
        System.out.println("************************** THRESHOLD ********************");
        StructField[] tmp2 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("countValue", DataTypes.DoubleType, true)
        };
        long ind = 1;DataFrame df2;DataFrame fromNumberMap;
        final long ind1 = ind;
        boolean flagLess = true; boolean flagMore = false;
        //int mentionThreshold = 73, fromThreshold = 73, hashtagThreshold = 50, termThreshold = 80;
        int fromThreshold= 159, mentionThreshold= 159, hashtagThreshold= 159, termThreshold= 159;
        DataFrame statVals;
        //ind += sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        //ind += sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        int indexNum = 1; long tmp;
        while(flagLess || flagMore) {
            ind = 1;
            if(indexNum != 1 && flagMore){
                //if(indexNum % 2 == 1) {
                fromThreshold+=1;
                termThreshold+=1;
                hashtagThreshold+=1;
                mentionThreshold+=1;
            }
            else if(indexNum != 1 && flagLess) {
                mentionThreshold -= 5;
                fromThreshold -= 5;
                hashtagThreshold -= 5;
                termThreshold -= 5;
            }

            System.out.println("================= INDEX NUMBER: " +indexNum + " FromThreshold: " + fromThreshold + " MentionThreshold: " + mentionThreshold + " hashtagThreshold: " + hashtagThreshold + " TermThreshold: " + termThreshold);


            df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
            fromNumberMap = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                    return new Tuple2<String, Double>(row.getString(1), 1.0);
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            }).map(new Function<Tuple2<String, Double>, Row>() {
                @Override
                public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                }
            }), new StructType(tmp2)).coalesce(numPart);
            /*if(indexNum == 1) {
                fromNumberMap.registerTempTable("countTable");
                System.out.println("=============== MIN MAX AVG FROM =======================");
                sqlContext.sql("SELECT COUNT(*), MIN(countValue), MAX(countValue), AVG(countValue) FROM countTable").show(4);
            }*/
            fromNumberMap = fromNumberMap.filter(fromNumberMap.col("countValue").$greater$eq(fromThreshold)).coalesce(numPart);
            tmp = fromNumberMap.count();
            ind += tmp;
            System.out.println("==========FINAL From COUNT============= " + tmp);
            System.out.println("================== IND VALUE AFTER FROM_FEATURE=================: " + ind);

            final long ind2 = ind;
            df2 = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);//.registerTempTable("tweetMention");
            fromNumberMap = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                    return new Tuple2<String, Double>(row.getString(1), 1.0);
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            }).map(new Function<Tuple2<String, Double>, Row>() {
                @Override
                public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                }
            }), new StructType(tmp2)).coalesce(numPart);
            /*if(indexNum == 1) {
                fromNumberMap.registerTempTable("countTable");
                System.out.println("=============== MIN MAX AVG TERM =======================");
                sqlContext.sql("SELECT COUNT(*), MIN(countValue), MAX(countValue), AVG(countValue) FROM countTable").show(4);
            }*/
            fromNumberMap = fromNumberMap.filter(fromNumberMap.col("countValue").$greater$eq(termThreshold)).coalesce(numPart);
            tmp = fromNumberMap.count();
            ind += tmp;
            System.out.println("==========FINAL TERM COUNT============= " + tmp);
            System.out.println("================== IND VALUE AFTER TERM_FEATURE=================: " + ind);

            final long ind3 = ind;
            df2 = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart);//.registerTempTable("tweetMention");
            fromNumberMap = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                    return new Tuple2<String, Double>(row.getString(1), 1.0);
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            }).map(new Function<Tuple2<String, Double>, Row>() {
                @Override
                public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                }
            }), new StructType(tmp2)).coalesce(numPart);
            /*if(indexNum == 1) {
                fromNumberMap.registerTempTable("countTable");
                System.out.println("=============== MIN MAX AVG HASHTAG =======================");
                sqlContext.sql("SELECT COUNT(*), MIN(countValue), MAX(countValue), AVG(countValue) FROM countTable").show(4);
            }*/
            fromNumberMap = fromNumberMap.filter(fromNumberMap.col("countValue").$greater$eq(hashtagThreshold)).coalesce(numPart);
            tmp = fromNumberMap.count();
            ind += tmp;
            System.out.println("==========FINAL HASHTAG COUNT============= " + tmp);
            System.out.println("================== IND VALUE AFTER HASHTAG_FEATURE=================: " + ind);

            final long ind4 = ind;
            df2 = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);//.registerTempTable("tweetMention");
            fromNumberMap = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                    return new Tuple2<String, Double>(row.getString(1), 1.0);
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            }).map(new Function<Tuple2<String, Double>, Row>() {
                @Override
                public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                }
            }), new StructType(tmp2)).coalesce(numPart);
            /*if(indexNum == 1) {
                fromNumberMap.registerTempTable("countTable");
                sqlContext.sql("SELECT COUNT(*), MIN(countValue), MAX(countValue), AVG(countValue) FROM countTable").show(4);
            }*/
            fromNumberMap = fromNumberMap.filter(fromNumberMap.col("countValue").$greater$eq(mentionThreshold)).coalesce(numPart);
            tmp = fromNumberMap.count();
            ind += tmp;
            System.out.println("==========FINAL Mention COUNT============= " + tmp);
            System.out.println("==========FINAL Feature COUNT============= " + (ind - 1));

            flagLess = false; flagMore = false;
            if(ind < featureNum + featureNumWin)
                flagLess = true;
            if(ind > featureNum + featureNumWin)
                flagMore = true;
            indexNum++;
        }


        System.out.println("======================== FOUND THE THRESHOLDS ===================== " + ind + " mentionThreshold: " + mentionThreshold + " fromThreshold: " + fromThreshold + " hashtagThreshold: " + hashtagThreshold + " termThreshold: " + termThreshold);
        /*df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        df2.unionAll(sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart));

        fromNumberMap = sqlContext.createDataFrame(df2.drop("tid").distinct().javaRDD().zipWithIndex().map(new Function<Tuple2<Row, Long>, Row>() {
            @Override
            public Row call(Tuple2<Row, Long> v1) throws Exception {
                return RowFactory.create(v1._1().getString(0), v1._2() + ind1);
            }
        }), new StructType(tmp)).coalesce(numPart);
        System.out.println("============ ALL USER's COUNT:" + fromNumberMap.count() + "=====================");
*/
        // getTweetTerm(sqlContext);
    }


    public static void writeHashtagBirthday(SQLContext sqlContext, final List<String> hashtagSet){
        StructField[] fields = {
                //DataTypes.createStructField("tid", DataTypes.StringType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        DataFrame df = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("tid").coalesce(numPart).distinct();
        /*sqlContext.createDataFrame(df.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return hashtagSet.contains(v1.getString(1));
            }
        }), new StructType(fields)).coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "tweet_hashtagSets_time_CSV");
*/

        sqlContext.createDataFrame(df.coalesce(numPart).distinct().javaRDD().mapToPair(new PairFunction<Row, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Row row) throws Exception {
                return new Tuple2<String, Long>(row.getString(0), row.getLong(1));
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aDouble, Long aDouble2) throws Exception {
                if(aDouble > aDouble2)
                    return aDouble2;
                else
                    return aDouble;
            }
        }).map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fields)).coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "tweet_hashtagSets_time_CSV");
    }

    public static void getHashtagPairFrequency(SQLContext sqlContext){
        StructField[] stField = {
                DataTypes.createStructField("hashtagPair", DataTypes.StringType, true),
                DataTypes.createStructField("countValue", DataTypes.DoubleType, true)
        };
        DataFrame df = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart);
        DataFrame df2 = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart);
        df = df.join(df2, df.col("tid").equalTo(df2.col("tid"))).drop(df.col("tid")).coalesce(numPart);
        df2 = sqlContext.createDataFrame(df.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {//tid, username, id
                return new Tuple2<String, Double>(row.getString(0) + "," + row.getString(2), 1.0);
            }
        }).reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble + aDouble2;
            }
        }).map(new Function<Tuple2<String, Double>, Row>() {
            @Override
            public Row call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                return !(row.getString(0).split(",")[0].equals(row.getString(0).split(",")[1]));
            }
        }), new StructType(stField)).coalesce(numPart);

        System.out.println("===================== HASHTAG PAIR FREQUENCY ===================== " + df2.count());
        df2.coalesce(1).write().mode(SaveMode.Overwrite).parquet(outputPath + "hashtagPair_frequency_parquet");
    }
}














