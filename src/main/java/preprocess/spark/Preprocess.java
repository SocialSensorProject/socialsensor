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
        boolean local = configRead.isLocal();
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
        if(local) {
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



        DataFrame mainData = null;
        if(local) {
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

        if(configRead.getTestTrainData()) {

            //thresholdMentionHashtagTermFeatures(sqlContext, sparkContext);
            //getGroupedMentionHashtagTermList(sqlContext, sparkContext);
            //getTweetTerm(sqlContext);

            getGroupedMentionHashtagTerm(sqlContext, sparkContext);
            //getTestTrainData(sqlContext);

            /*for (int gNum = 1; gNum <= 2; gNum++) {
                groupNum = gNum;
                System.out.println("==================== Group Num: "+groupNum+"===================");
                System.out.println("==================== ENTERING TWEET TOPICAL===================");
                if(gNum > 2)
                    getTweetTopical(sqlContext);
                System.out.println("==================== ENTERING TEST TRAIN DATA WITH TOPICAL===================");
                getTestTrainDataSet(sqlContext);
            }*/

            //writeAsCSV(sqlContext);
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
        final List<String> hashtagList = getGroupHashtagList(groupNum);
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("topical", DataTypes.IntegerType, true)
        };
        DataFrame df = sqlContext.createDataFrame(sqlContext.read().parquet(outputPath + "tweet_user_hashtag_grouped_parquet").drop("username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
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
        DataFrame negativeSamples, positiveSamples;
        // negativeSamples = df.filter(df.col("topical").$eq$eq$eq(0)).coalesce(numPart);
        // positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);

        System.out.println("================== TWEET TOPICAL COUNT: " + df.count() + "========================");
        //System.out.println("================== TWEET TOPICAL POSITIVE COUNT: " + positiveSamples.count() + "========================");
        //System.out.println("================== TWEET TOPICAL NEGATIVE COUNT: " + negativeSamples.count() + "========================");
        //df.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_topical_allTweets_" + groupNum + "_parquet");

        String name = "tweet_hashtag_user_mention_term_time_parquet";
        if(allInnerJoin)
            name = "tweet_hashtag_user_mention_term_time_allInnerJoins_parquet";
        DataFrame df2 = sqlContext.read().parquet(outputPath + name).drop("user").drop("hashtag").drop("term").drop("mentionee").drop("time").drop("username").coalesce(numPart);//.registerTempTable(
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
        df.write().mode(SaveMode.Overwrite).parquet(outputPath + name + groupNum + "_parquet");
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
        DataFrame df1 = sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart);//.registerTempTable("tweetUser");
        DataFrame df2 = sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").coalesce(numPart);//.registerTempTable("tweetTerm");"tweetHashtag");
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
        //df1.write().parquet(outputPath + "tweet_tmp1_parquet");

        //df1 = sqlContext.read().parquet(outputPath + "tweet_tmp1_parquet");
        System.out.println("================== TMP1 COUNT: =========== " + df1.count());
        df2 = sqlContext.read().parquet(outputPath + "tweet_hashtagFeature_grouped_parquet").coalesce(numPart);//.registerTempTable(
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
        //df1.write().parquet(outputPath + "tweet_tmp2_parquet");

        //df1 = sqlContext.read().parquet(outputPath + "tweet_tmp2_parquet");
        df2 = sqlContext.read().parquet(outputPath + "tweet_mentionFeature_grouped_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
        //System.out.println("================== COUNT: =========== " + df1.count());
        //df1.cache();

        df2 = sqlContext.read().parquet(outputPath + "tweet_time_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
        df1.printSchema();
        //df1.show(100);
        System.out.println("================== FINAL TWEET COUNT: =========== " + df1.count());
        output(df1, "tweet_hashtag_user_mention_term_time_allInnerJoins", false);
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
        final int fromThreshold= 137, mentionThreshold= 213, hashtagThreshold= 123, termThreshold= 246;
        //ind += sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        //ind += sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        DataFrame fromNumberMap;
        DataFrame df1;
        df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);//.registerTempTable("tweetMention");

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
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("username").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("username")).coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getLong(0), String.valueOf(v1.getLong(1)));
            }
        }), new StructType(fieldsFrom));
        //output(df2, "tweet_fromFeature_grouped", false);
        System.out.println("==================DOUBLE CHECK SIZES=================: " + df2.count());
        System.out.println("================== IND VALUE AFTER FROM_FEATURE=================: " + ind);

        final long ind2 = ind;
        df2 = sqlContext.read().parquet(outputPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);//.registerTempTable("tweetMention");
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
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("term").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("term")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), String.valueOf(row.getLong(1)));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String aDouble, String aDouble2) throws Exception {
                return aDouble + " " +  aDouble2;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fieldsTerm)).coalesce(numPart);
        //output(df2, "tweet_termFeature_grouped", false);
        //System.out.println("==========FINAL TERM COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER TERM_FEATURE=================: " + fromNumberMap.count());

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
        df2 = sqlContext.createDataFrame(df2.join(fromNumberMap, df2.col("hashtag").equalTo(fromNumberMap.col("username"))).drop(fromNumberMap.col("username")).drop(df2.col("hashtag")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Row row) throws Exception {
                return new Tuple2<Long, String>(row.getLong(0), String.valueOf(row.getLong(1)));
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String aDouble, String aDouble2) throws Exception {
                return aDouble + " " +  aDouble2;
            }
        }).map(new Function<Tuple2<Long, String>, Row>() {
            @Override
            public Row call(Tuple2<Long, String> stringDoubleTuple2) throws Exception {
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
            }
        }), new StructType(fieldsHashtag)).coalesce(numPart);
        //output(df2, "tweet_hashtagFeature_grouped", false);

        final long ind4 = ind;
        //System.out.println("==========FINAL HASHTAG COUNT============= " + df2.count());
        System.out.println("================== IND VALUE AFTER HASHTAG_FEATURE=================: " + fromNumberMap.count());

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
        ind += fromNumberMap.count();
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
        //output(df2, "tweet_mentionFeature_grouped", false);
        System.out.println("==========FINAL Mention COUNT============= " + fromNumberMap.count());
        System.out.println("==========FINAL Feature COUNT============= " + (ind-1));
    }

    public static void getGroupedTweetTermHashtag(DataFrame tweet_text, SQLContext sqlContext){
        StructField[] fields1 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("term", DataTypes.StringType, true),
                //DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        final String emo_regex2 = "([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee])";//"\\p{InEmoticons}";
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

        sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart).registerTempTable("userTweet");
        DataFrame df1 = sqlContext.sql("SELECT username, count(tid) AS tweetCount from userTweet GROUP BY username").coalesce(numPart);
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
        System.out.println("===================NUMBER OF FEATURES: " + sqlContext.read().parquet(outputPath + "tweet_mentionFeature_grouped_parquet").drop("tid").distinct().unionAll(sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").drop("tid").distinct()).unionAll(sqlContext.read().parquet(outputPath + "tweet_hashtagFeature_grouped_parquet").drop("tid").distinct()).unionAll(sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").drop("tid").distinct()).coalesce(numPart).count());
        DataFrame df = sqlContext.read().parquet(outputPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").distinct().coalesce(numPart);
        System.out.println("COUNT: " + df.count());
        //df.cache();
        df.coalesce(40).write().parquet(dataPath + "tweet_term_parquet");
    }

    private static List<String> getGroupHashtagList(int groupNum) {
        List<String> hashtagList = new ArrayList<>();
        if(configRead.isLocal()){
            hashtagList.add("h1");
            hashtagList.add("h5");
            hashtagList.add("h9");
        }else {
            String hashtagStrList = "";
            if (groupNum == 1)
                hashtagStrList = "ebola,prayforsouthkorea,haiyan,prayforthephilippines,ukstorm,pray4philippines,yycflood,abflood,mers,ebolaresponse,ebolaoutbreak,kashmirfloods,icestorm2013,h7n9,coflood,typhoon,boulderflood,typhoonhaiyan,dengue,serbiafloods,bayareastorm,hagupit,hellastorm,ukfloods,chikungunya,rabies,bosniafloods,h1n1,birdflu,chileearthquake,artornado,napaearthquake,evd68,arkansastornado";
            else if (groupNum == 2) // POLITICS
                hashtagStrList = "gazaunderattack,sotu,mh17,bringbackourgirls,snowden,blacklivesmatter,fergusondecision,gunsense,crimea,peshawarattack,election2014,governmentshutdown,netneutrality,standwithrand,hobbylobby,popefrancis,standwithwendy,defundobamacare,illridewithyou,bringbackourmarine,irantalks,newnjgunlaws,gopshutdown,prop8,fergusonoctober,occupycentral,navyyardshooting,bridgegate,israelunderfire,dearpopefrancis,bbcindyref,daesh,11mayprotests,prayformh17,ottawashooting,gopout,stopmarriagebill,occupyhk,stopthegop,votegunsense,newpope,shawshooting,umbrellarevolution,gazaunderfire,irantalksvienna,obamacareinthreewords,floodwallstreet,relegalize,1988iranmassacre,nonucleariran,midtermelections,europeanelections,afghanelections,ceasefirenow,occupygezy";
            else if (groupNum == 3) // GENERIC (1044 REDA HASHTAGS)
                hashtagStrList = "1000daysof1d,100cancionesfavoritas,100happydays,10thingsimattractedto,10thingsyouhatetodo,11million,14daysoffifa,1bigannouncement,1dalbumfour,1dalbumfourfollowspree,1dbigannouncement,1dconcertfilmdvd,1dday,1ddayfollowparty,1ddayfollowspree,1ddaylive,1defervescenciaccfm,1dfireproof,1dfragrance,1dmoviechat,1dmovieff,1dmoviepremiere,1dorlando,1dproposal,1dthisisus,1dthisisusff,1dtoday,1dtuesdaytest,1dwwafilm,1dwwafilmtrailer,2013follow,2013taughtme,2014in5words,2014mama,20cancionesfavoritas,20grandesquemellevodel2013,20personasimportantesenmivida,22personasespeciales,24lad,24seven,25m,3000miles,3daysforexpelledmovie,3daystill5hboss,3yearsofonedirection,40principales1d,42millionbeliebers,44millionbeliebers,44millionbeliebersfollowparty,4musiclfsbeliebers,4musiclfsdirectioners,4musiclfslittlemonsters,4thofjuly,4yearsago5strangersbecame5brothers,4yearsof1d,4yearsof1dfollowspree,4yearsofonedirection,5countries5days,5daysforexpelledmovie,5hcountdowntochristmas,5millionmahomies,5moresecondsofsummer,5sosalbumfollowspree,5sosderpcon,5sosdontstopfollowme,5sosdontstopfollowspree,5sosfambigvote,5sosfamilyfollowspree,5sosgot2millionfollowersfollowparty,5sosupindisstream,5sosvotinghour,5wordsihatetohear,69factsaboutme,6thfan,7millionmahomies,7yearsofkidrauhl,8daysforexpelledmovie,aaandrea,aadian,aaliyahmovie,aaronsfirstcover,aaronto1m,aaronto600k,aaronto700k,aaronto800k,aatwvideo,accela,actonclimate,actonreform,addawordruinamovie,advancedwarfare,aeronow,afazenda,aga3,agentsofshield,ahscoven,ahsfreakshow,albert_stanlie,alcal100k,alcs,alexfromtarget,alfredo1000,aliadosmusical,alitellsall,allday1ddayfollowspree,allieverneed,allonabilla,allthatmattersmusicvideo,allyfollowme,allylovesyou,alsicebucketchallenge,altband,alwayssupportluhan,amas2014,amazoncart,americanproblemsnight,andreganteng,angelatorres,anotherfollowtrain,anthonytuckerca,applausevid819,applelive,applepay,aprilwishes,areyoutheone,arianagrandeptw,arianalovesyou,ariananow,arianatorshelparianators,artistoftheyearhma,artrave,ash5sosfollowme,askarianagrande,askkendallschmidt,askmiley,asknash,asktyleranything,austinto6m,austinto6million,automaticturnons,azadimarchpti,azadisquare,bail4bapuji,bailon7thjan,bamforxmas,bamshiningstar,bangabanga,bangerztour,bap1004,bapuji,batalla_alboran,batimganteng,bb16,bb8,bbathechase,bbcan2,bbhotshots,bblf,bbma,bbmas,bbmzansi,beafanday,believeacousticforgrammy,believemovieposter,believemovieweekend,believepremiere,bellletstaik,bestcollaboration,bestfandom,bestfandom2014,bestplacetolistentobestsongever,bestsongevermusicvideotodayfollowparty,betawards2014,bethanymotacollection,bethanymotagiveaway,bieberchristmas,blacklivesmatter,bluemoontourenchile,bostonstrong,brager,bravsger,brazilvsgermany,breall,brelandsgiveaway,brentrivera,brentto300k,bringbackourgirls,bringbackourmarine,britishband,britsonedirection,buissness,bundyranch,buybooksilentsinners,buyproblemonitunes,bythewaybuytheway,cabletvactress,cabletvdrama,caiimecam,cailmecam,calimecam,callmesteven,cameronmustgo,camfollowme,camilacafollowspree,camilachameleonfollowspree,camilalovesfries,camilasayshi,camsbookclub,camsnewvideo,camsupdatevideo,camto1mill,camto2mil,camto2million,camto4mil,camto4mill,camwebstartca,candiru_v2,caoru,caraquici,carinazampini,cartahto1mil,carterfollowme,carterfollowspree,cartersnewvideo,carterto1mil,carterto1million,carterto300k,cashdash,cashnewvideo,cdm2014,ces2014,cfclive,changedecopine,childhoodconfessionnight,christmasday,christmasrocks,closeupforeversummer,clublacura,codghosts,colorssavemadhubalaeiej,comedicmovieactress,comedictvactor,comedictvactress,cometlanding,comiczeroes,conceptfollowspree,confessyourunpopularopinion,confidentvideo,congrats5sos,connorhit2million,connorto800k,contestentry,copyfollowers,cr4u,crazymofofollowspree,crazymofosfollowparty,crazymofosfollowspree,criaturaemocional,crimea,crimingwhilewhite,csrclassics,daretozlatan,darrenwilson,davidables,dday70,defundobamacare,del40al1directionersdream,demandavote,demiversary,demiworldtour,dhanidiblockricajkt48,dianafollowparty,didntgetaniallfollowfollowparty,directionermix1065,directionersandbeliebersfollowparty,directvcopamundial,disneymarvelaabcd,djkingassassin,dodeontti,dogecoin,donaldsterling,donetsk,dontstop5sos,dontstopmusicvideo,drqadri,drunkfilms,dunntrial,e32014,educadoresconlahabilitante,educatingyorkshire,elipalacios,emaazing,emabigge,emabiggestfan,emabiggestfans,emabiggestfans1d,emabiggestfans1dᅠ,emabiggestfans5sos,emabiggestfansarianagrande,emabiggestfansj,emabiggestfansju,emabiggestfansjustinbieber,encorejkt48missionst7,entrechavistasnosseguimos,epicmobclothing,ericgarner,ericsogard,esimposiblevivirsin,esurancesave30,etsymnt,euromaidan,eurovisionsongcontest2014,eurovisiontve,everybodybuytheway,exabeliebers,exadirectioners,exarushers,expelledmovie,expelledmovietonumberone,experienciaantiplan,facetimemecam,factsonly,fairtrial4bapuji,fake10factsaboutme,fakecases,fallontonight,fanarmy,fandommemories2013,farewellcaptain,fatalfinale,faze5,fergusondecision,fetusonedirectionday,ffmebellathorne,fictionalcharactersiwanttomarry,fictionaldeathsiwillnevergetover,fifa15,filmfridays,finallya5sosalbum,finalride,findalice,findingcarter,folllowmecam,follobackinstantly,follow4followed,followcarter,followella,followerscentral,followeverythinglibra,followliltwist,followmeaustincarlile,followmebefore2014,followmebrent,followmecarter,followmecon,followmeconnor,followmehayes,followmejack,followmejg,followmejoshujworld,followmelittlemixstudio,followmenash,followmeshawn,followmetaylor,followpyramid,followtrick,fordrinkersonly,fourhangout,fourtakeover,freebiomass,freejustina,freenabilla,freethe7,funnyonedirectionmemories,funwithhashtag,fwenvivoawards,gabesingin,gamergate,geminisweare,georgeujworld,gerarg,gervsarg,getbossonitunes,getcamto2mil,getcamto2million,getcamto3mil,getcamto3mill,getcamto800k,getcovered,getsomethingbignov7,gha,gigatowndun,gigatowndunedin,gigatowngis,gigatownnsn,gigatowntim,gigatownwanaka,givebackphilippines,gleealong,globalartisthma,gmff,gobetter,gobiernodecalle,goharshahi,gonawazgo,goodbye2013victoria,got7,got7comeback,gotcaketour2014,governmentshutdown,gpawteefey,gpettoe_is_a_scammer,grandtheftautomemories,greenwall,gtaonline,h1ddeninspotify,h1ddeninspotifydvd,haiyan,handmadehour,happybirthdaybeliebers,happybirthdaylouis,happybirthdayniall,happybirthdaytheo,happyvalentines1dfamily,harryappreciationday,hastasiemprecerati,havesandhavenots,hayesnewvideo,hayesto1m,hearthstone,heat5sos,heatjustinbieber,heatonedirection,heatplayoffs,heforshe,hermososeria,hey5sos,hiari,hicam,himymfinale,hiphopawards,hiphopsoty,hollywoodmusicawards,hometomama,hoowboowfollowtrain,hormonesseason2,houston_0998,hr15,hrderby,htgawm,iartg,icc4israel,icebucketchallenge,ifbgaintrain,igetannoyedwhenpeople,iheartawards,iheartmahone,illridewithyou,imeasilyannoyedby,immigrationaction,imniceuntil,imsotiredof,imsousedtohearing,imthattypeofpersonwho,imtiredofhearing,incomingfreshmanadvice,indiannews,inners,intense5sosfamattack,inthisgenerationpeople,ios8,iphone6plus,irememberigotintroublefor,isabellacastilloporsolista,isacastilloporartista,isil,italianmtvawards,itzbjj,iwanttix,iwishaug31st,jackto500k,jalenmcmillan,james900k,jamesfollow,jamesto1m,jamesyammounito1million,janoskianstour,jcfollowparty,jcto1million,jcto700k,jerhomies,jjujworld,joshujworld,justiceformikebrown,justinfollowalljustins,justinformmva,justinmeetanita,justwaitonit,kasabi,kaththi,kcaargentina,kcaᅠ,kcaméxico,kcamexico,kedsredtour,kellyfile,kikifollowspree,kingbach,kingofthrones,kingyammouni,knowthetruth,kobane,kykaalviturungunung,laborday,lacuriosidad,lastnightpreorder,latemperatura,leeroyhmmfollowparty,lesanges6,letr2jil,lhhatlreunion,lhhhollywood,liamappreciationday,libcrib,liesivetoldmyparents,lifewouldbealotbetterif,lindoseria,linesthatmustbeshouted,littlemixsundayspree,livesos,lollydance,longlivelongmire,lopopular2014,lorde,losdelsonido,louisappreciationday,lovemarriottrewards,mabf,macbarbie07giveaway,madeinaus,madisonfollowme,magconfollowparty,mahomiesgohardest,makedclisten,malaysiaairlines,maleartist,mama2013,mandelamemorial,mariobautista,marsocial,martinastoessel,maryamrajavi,mattto1mil,mattto1mill,mattto2mill,mattyfollowspree,mchg,meetthevamily,meetthevamps,meninisttwitter,mentionadislike,mentionpeopleyoureallylove,mentionsomeoneimportantforyou,mercedeslambre,merebearsbacktoschool,metrominitv,mgwv,mh17,mh370,michaelbrown,michaelisthebestest,midnightmemories,midnightmemoriesfollowparty,mileyformmva,mipreguntaes,miprimertweet,mis10confesiones,mis15debilidades,missfrance2014,missfrance2015,mixfmbrasil,mmlp2,mmva,monstermmorpg,monumentour,moremota,motafam,motatakeover,movieactress,movimentocountry,mplaces,mpointsholiday,mrpoints,mtvclash,mtvh,mtvho,mtvhot,mtvhott,mtvhotte,mtvhottes,mtvkickoff,mtvmovieawards,mtvs,mtvst,mtvsta,mtvstar,mtvsummerclash,mufclive,mufflerman,multiplayercomtr,murraryftw,murrayftw,musicjournals,my15favoritessongs,myboyfriendnotallowedto,myfourquestion,mygirlfriendnotallowedto,mynameiscamila,myxmusicawards,my_team_pxp,nabillainnocente,nairobisc,nakedandafraid,nash2tomil,nashsnewvid,nashsnewvideo,nashto1mill,nashto2mil,nblnabilavoto,neonlightstour,networktvcomedy,net_one,neversurrenderteam,newbethvideo,newsatquestions,newyearrocks,nhl15bergeron,nhl15duchene,nhl15oshie,nhl15subban,niallappreciationday,niallsnotes,nightchanges,nightchangesvideo,nionfriends,nj2as,no2rouhani,nominateaustinmahone,nominatecheryl,nominatefifthharmony,nominatethevamps,nonfollowback,noragrets,notaboyband,notersholiday2013,nothumanworld,noticemeboris,notyourshield,nowmillion,nowplayingjalenmcmillanuntil,nudimensionatami,nwts,o2lfollowparty,o2lhitamillion,occupygezi,officialsite,officialtfbjp,oitnb,onedirectionencocacolafm,onedirectionformmva,onedirectionptw,onedirectionradioparty,onemoredaysweeps,onenationoneteam,onsefollowledimanchesanspression,operationmakebiebersmile,oppositeworlds,opticgrind,orangeisthenewblack,orianasabatini,oscartrial,othertech,pablomartinez,pakvotes,parentsfavoriteline,paulachaves,paulafernandes,pcaforsupernatural,pdx911,peachesfollowtrain,pechinoexpress,peligrosincodificar,peopieschoice,peopleireallywanttomeet,peoplewhomademyyeargood,perduecrew,perfectonitunes,perfectoseria,peshawarattack,peterpanlive,playfreeway,pleasefollowmecarter,popefrancis,postureochicas,pradhanmantri,praisefox,prayforboston,prayformh370,prayforsouthkorea,prayforthephilippines,praytoendabortion,preordershawnep,pricelesssurprises,queronotvz,qz8501,randbartist,rapgod,raplikelilwayne,rbcames,rcl1milliongiveaway,rdmas,re2pect,realliampaynefollowparty,redbandsociety,relationshipgoals,rememberingcory,renewui,renhotels,replacemovietitleswithpope,retotelehit,retotelehitfinalonedirection,retweetback,retweetfollowtrain,retweetsf,retweetsfo,retweetsfollowtrain,rickychat,ripcorymonteith,riplarryshippers,ripnelsonmandela,rippaulwalker,riprobinwilliams,riptalia,ritz2,rmlive,rollersmusicawards,sammywilk,sammywilkfollowspree,samwilkinson,savedallas,sbelomusic,sbseurovision,scandai,scandalfinale,scarystoriesin5words,scifiactor,scifiactress,scifitv,selenaformmva,selenaneolaunch,selffact,setting4success,sexylist2014,sh0wcase,shareacokewithcam,sharknado,sharknado2,sharknado2thesecondone,shawnfollowme,shawnsfirstsingle,shawnto1mil,shawnto500k,shelookssoperfect,sherlocklives,shotsto600k,shotties,shouldntcomeback,simikepo,simplementetini,skipto1mill,skywire,smoovefollowtrain,smpn12yknilaiuntertinggi,smpn12yksuksesun,smurfsvillage,smurfvillage,sobatindonesia,socialreup,somebodytobrad,somethingbigatmidnight,somethingbigishappening,somethingbigishappeningnov10,somethingbigtonumber1,somethingbigvideo,somethingthatwerenot,sometimesiwishthat,sonic1d,sosvenezuela,soydirectioner,spamansel,spinnrtaylorswift,sportupdate,spreadcam,spreadingholidaycheerwithmere,ss8,ssnhq,standwithrand,standwithwendy,starcrossed,staystrongexo,stealmygiri,stealmygirl,stealmygirlvevorecord,stealmygirlvideo,stilababe09giveaway,storm4arturo,storyofmylife16secclip,storyofmylifefollowparty,storyofmylifefollowspree,summerbreaktour,superjuniorthelastmanstanding,supernaturai,sydneysiege,takeoffjustlogo,talklikeyourmom,talktomematt,tampannbertanya,taylorto1m,taylorto1mill,taylorto58285k,taylorto900k,tayto1,tcas2014,tcfollowtrain,teamfairyrose,teamfollowparty,teamfree,teamgai,teamrude,techtongue,teenawardsbrasil,teenchoice,telethon7,tfb_cats,thanksfor3fantasticyears1d,thankyou1d,thankyou1dfor,thankyoujesusfor,thankyouonedirectionfor,thankyousachin,thankyousiralex,that1dfridayfeelingfollowspree,thatpower,the100,thebiggestlies,theconjuring,thefosterschat,thegainsystem,thegifted,thehashtagslingingslasher,themonster,themostannoyingthingsever,thepointlessbook,thereisadifferencebetween,thesecretonitunes,thevamps2014,thevampsatmidnight,thevampsfollowspree,thevampssaythanks,thevampswildheart,theyretheone,thingsisayinschoolthemost,thingsiwillteachmychild,thingspeopledothatpissmeoff,thisisusfollowparty,thisisusfollowspree,threewordsshewantstohear,threewordstoliveby,tiannafollowtrain,time100,timepoy,tinhouse,tipsfornewdirectioners,tipsforyear7s,titanfall,tityfolllowtrain,tixwish,tns7,tntweeters,tokiohotelfollowspree,tomyfuturepartner,tonyfollowtrain,topfiveunsigned,topfollow,topfollowback,topretw,topretweet,topretweetgaintra,topretweetgaintrai,topretweetgaintrain,topretweetmax,toptr3ce,torturereport,totaldivas,toydefense2,tracerequest,trevormoranto500k,trndnl,truedetective,ts1989,tvbromance,tvcrimedrama,tvgalpals,tvtag,tweeliketheoppositegender,tweetlikejadensmith,tweetliketheoppositegender,tweetmecam,tweetsinchan,twitterfuckedupfollowparty,twitterpurge,uclfinal,ufc168,ultralive,unionjfollowmespree,unionjjoshfollowspree,unionjustthebeginning,unitedxxvi,unpopularopinionnight,uptime24,usaheadlines,user_indonesia,utexaspinkparty,vampsatmidnight,verifyaaroncarpenter,verifyhayesgrier,vevorecord,vincicartel,vinfollowtrain,visitthevamps,vmas2013,voiceresults,voicesave,voicesaveryan,vote4austin,vote4none,vote5hvma,vote5hvmas,vote5os,vote5sosvmas,voteaugust,votebilbo,voteblue,votecherlloyd,votedemilovato,votedeniserocha,votefreddie,votegrich,votejakemiller,votejennette,votejup,votekatniss,votekluber,voteloki,voteluan,votematttca,votemiley,votemorneau,voterizzo,votesamandcat,votesnowwhite,votesuperfruit,votetimberlake,votetris,votetroyesivan,voteukarianators,voteukdirectioners,voteukmahomies,votevampsvevo,voteveronicamars,votezendaya,waliansupit,weakfor,weareacmilan,weareallharry,weareallliam,weareallniall,weareallzayn,wearewinter,webeliveinyoukris,wecantbeinarelationshipif,wecantstop,welcometomyschoolwhere,weloveyouliam,wethenorth,wewantotrainitaly2015,wewantotratourinitaly2015,wewillalwayssupportyoujustin,whatidowheniamalone,wheredobrokenheartsgo,whereismike,wherewearetour,whitebballpains,whitepeopleactivities,whowearebook,whybeinarelationshipif,whyimvotingukip,wicenganteng,wildlifemusicvideo,wimbledon2014,win97,wmaexo,wmajustinbieber,wmaonedirection,womaninbiz,wordsonitunestonight,worldcupfinal,worldwara,wwadvdwatchparty,wwat,wwatour,wwatourfrancefollowspree,xboxone,xboxreveal,xf7,xf8,xfactor2014,xfactorfinal,yammouni,yeremiito21,yesallwomen,yespimpmysummerball,yespimpmysummerballkent,yespimpmysummerballteesside,yolandaph,youmakemefeelbetter,younusalgohar,yourstrulyfollowparty,ytma,z100jingleball,z100mendesmadness,zaynappreciationday,zimmermantrial,__tfbjp_";
            Collections.addAll(hashtagList, hashtagStrList.split(","));
        }
        return hashtagList ;
    }

    public static void getGroupedMentionHashtagTermList(SQLContext sqlContext, JavaSparkContext sc){
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
        StructField[] tmp = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("id", DataTypes.LongType, true)
        };
        StructField[] tmp2 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("countValue", DataTypes.DoubleType, true)
        };
        long ind = 1;DataFrame df2;DataFrame fromNumberMap;
        final long ind1 = ind;
        boolean flagLess = true; boolean flagMore = false;
        //int mentionThreshold = 73, fromThreshold = 73, hashtagThreshold = 50, termThreshold = 80;
        int mentionThreshold = 0, fromThreshold = 0, hashtagThreshold = 0, termThreshold = 0;
        DataFrame statVals;
        //ind += sqlContext.read().parquet(outputPath + "tweet_fromFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        //ind += sqlContext.read().parquet(outputPath + "tweet_termFeature_grouped_parquet").coalesce(numPart).count();
        //System.out.println("*****************IND: " + ind + " ************************");
        int indexNum = 1;
        while(flagLess || flagMore) {
            ind = 1;
            if(indexNum == 1){;//2) {
                // FromThreshold: 138 MentionThreshold: 213 hashtagThreshold: 123 TermThreshold: 246
                fromThreshold= 137; mentionThreshold= 213; hashtagThreshold= 123; termThreshold= 246;
            }
            if(indexNum != 1 && flagMore){
                //if(indexNum % 2 == 1) {
                fromThreshold+=4;
                termThreshold+=4;
                hashtagThreshold+=4;
                mentionThreshold+=8;
            }
            else if(indexNum != 1 && flagLess) {
                if(mentionThreshold > 1.5*fromThreshold)
                    mentionThreshold--;
                fromThreshold--;
                hashtagThreshold--;
                termThreshold--;
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
            ind += fromNumberMap.count();
            System.out.println("==========FINAL From COUNT============= " + fromNumberMap.count());
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
            ind += fromNumberMap.count();
            System.out.println("==========FINAL TERM COUNT============= " + fromNumberMap.count());
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
            ind += fromNumberMap.count();
            System.out.println("==========FINAL HASHTAG COUNT============= " + fromNumberMap.count());
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
            ind += fromNumberMap.count();
            System.out.println("==========FINAL Mention COUNT============= " + fromNumberMap.count());
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

}














