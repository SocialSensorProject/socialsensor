package machinelearning.naiveBayes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import util.ConfigRead;
import scala.Tuple2;
import util.TweetUtil;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Created by zahraiman on 8/10/15.
 */
public class ComputeNBLogOdds {
    private static String hdfsPath;
    private static int numPart;
    private static double tweetCount;
    private static SQLContext sqlContext;
    private static DataFrame tweet_user_hashtag_grouped;
    private static DataFrame tweet_hashtag_hashtag_grouped;
    private static DataFrame tweet_mention_hashtag_grouped;
    private static DataFrame tweet_term_hashtag_grouped;
    private static DataFrame tweet_location_hashtag_grouped;
    private static DataFrame tweetTime;
    private static DataFrame fromUserProb;
    private static DataFrame containTermProb;
    private static DataFrame toUserProb;
    private static DataFrame containLocationProb;
    private static DataFrame fromHashtagProb;
    private static boolean testSet = false;
    private static int topUserNum;
    private static boolean localRun;
    private static ConfigRead configRead;
    private static JavaSparkContext sparkContext;
    private static long [] containNotContainCounts;
    private static String dataPath;
    private static String outputPath; //"Local_Results/out/";
    private static final boolean calcFromUser = true;
    private static final boolean calcToUser = true;
    private static final boolean calcContainHashtag = true;
    private static final boolean calcContainTerm = true;
    private static final boolean calcContainLocation = true;
    private static int groupNum;
    private static int numOfGroups;
    private static String[] groupNames;
    private static TweetUtil tweetUtil = new TweetUtil();
    //private static DataFrame tweetTime;
    private static final int topFeatureNum = 1000;
    private static boolean testFlag;
    private static final double lambda1 = 1.0E-15;
    private static final double lambda2 = 1.0E-8;
    private static final double lambda3 = 0.001;
    private static final double lambda4 = 1.0;
    private static final double lambda5 = 1.0E-20;
    private static final double lambda6 = 0.1;
    private static boolean computeTweetLocation = false;
    private static List<List<String>> milionFeatureLists;
    private static List<Long> twoMilionIdList;
    public static double kValue;
    public static long timestamp;
    private static String testVal;
    private static String classname;
    private static HashSet<String> allHashtags;

    //private static final long[] timestamps= {1377897403000l, 1362146018000l, 1391295058000l, 1372004539000l, 1359920993000l, 1364938764000l, 1378911100000l, 1360622109000l, 1372080004000l, 1360106035000l};;


    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
        testFlag = configRead.getTestFlag();
    }

    public static void ComputeLogOdds(String _className, int _groupNum, String featurePath, double _kValue, long _splitTime, String _dataPath, HashSet<String> _hashtagList, String _testVal) throws IOException, ParseException {
        testVal = _testVal;
        classname = _className;
        allHashtags = _hashtagList;
        timestamp = _splitTime;
        kValue = _kValue;
        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        if(testFlag){
            timestamp = format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime();
        }
        groupNum = _groupNum;
        loadConfig();
        //long t1 = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse("Fri Feb 28 11:00:00 +0000 2014").getTime();
        numOfGroups = configRead.getNumOfGroups();
        groupNames = configRead.getGroupNames();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();

        outputPath = hdfsPath + configRead.getOutputPath();
        localRun = configRead.isLocal();
        topUserNum = configRead.getTopUserNum();
        TweetUtil tweetUtil = new TweetUtil();
        milionFeatureLists = tweetUtil.get1MFeatures(localRun, featurePath);

        dataPath = hdfsPath + configRead.getDataPath() + configRead.getGroupNames()[groupNum-1] + "/fold0/Ids/";

        dataPath = _dataPath;
        outputPath = "/data/ClusterData/input/Data/";
        if(testFlag)
            outputPath += "test/";

        //twoMilionIdList = tweetUtil.get2MTweetIds(localRun, configRead.getGroupNames()[groupNum-1]);
        initializeSqlContext();
        System.out.println("============================"+groupNum+"=============================");
        if(calcToUser)
            calcToUserProb(tweetCount, groupNum);

        if(calcContainHashtag)
            calcContainHashtagProb(tweetCount, groupNum);

        if(calcFromUser)
            calcFromUserProb(tweetCount);

        if(calcContainTerm)
            calcContainTermProb(tweetCount, groupNum);

        if(calcContainLocation) {
            calcContainLocationProb(tweetCount, groupNum);
        }

//            containNotContainCounts = getContainNotContainCounts(groupNum);

        if (calcToUser)
            calcTweetCondToUserConditionalEntropy(groupNum);

        if (calcContainHashtag)
            calcTweetCondContainHashtagConditionalEntropy(groupNum);

        if (calcFromUser)
            calcTweetCondFromUserConditionalEntropy(groupNum);

        if (calcContainTerm)
            calcTweetCondContainTermConditionalEntropy(groupNum);

        if(calcContainLocation)
            calcTweetCondContainLocationConditionalEntropy(groupNum);
        sparkContext.close();
    }

    public static void initializeSqlContext() throws IOException {
        SparkConf sparkConfig;
        if(localRun) {
            if(testFlag) {
                dataPath = configRead.getTestDataPath();
                //dataPath = "TestSet/data1Month/";
                outputPath = configRead.getTestOutPath();
            }
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[2]").set("spark.executor.memory", "6g").set("spark.driver.maxResultSize", "6g");
        }else {
            //tweetCount = 829026458; //tweet_user.count();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[8]");
        }
        sparkContext = new JavaSparkContext(sparkConfig);
        sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);
        System.out.println("======================GROUPNUM: " + groupNum + dataPath + "tweet_time_parquet");
        final int grNum = groupNum;
        /*tweetTime = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);
        StructField[] timeField = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        tweetTime = sqlContext.createDataFrame(tweetTime.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getLong(1) <= timestamps[grNum-1];
            }
        }), new StructType(timeField)).coalesce(numPart);
        tweetTime.cache();
        tweetCount = tweetTime.count();*/
        //System.out.println("=================================Tweet Time Size: " + tweetCount);
        tweet_user_hashtag_grouped = sqlContext.read().parquet(dataPath+ classname + "/fold0/Ids/" + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);
        final List<Long> idList = twoMilionIdList;
//        tweet_user_hashtag_grouped = tweet_user_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_user_hashtag_grouped.col("tid")), "inner").drop(tweetTime.col("tid")).coalesce(numPart);
        if(testFlag){
            //tweetTime = sqlContext.read().parquet("/data/ClusterData/input/TestSet/Data/" + "tweet_time_parquet").coalesce(numPart);
            //tweet_user_hashtag_grouped = tweet_user_hashtag_grouped.join(tweetTime, tweet_user_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
        }
        tweet_user_hashtag_grouped = sqlContext.createDataFrame(tweet_user_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return  (v1.getLong(3) <= timestamp);
            }
        }), tweet_user_hashtag_grouped.schema());
        //Compute topical and notTopical counts before filtering tweet_user_hashtag table
        containNotContainCounts = getContainNotContainCounts(groupNum);
        tweetCount = tweet_user_hashtag_grouped.count();

        final List<String> fromList = milionFeatureLists.get(0);
        if(!testFlag) {
            tweet_user_hashtag_grouped = sqlContext.createDataFrame(tweet_user_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return fromList.contains(v1.getString(1));
                }
            }), tweet_user_hashtag_grouped.schema());
        }
        if(computeTweetLocation){
            StructField[] fieldsLocation = {
                    DataTypes.createStructField("tid", DataTypes.LongType, true),
                    DataTypes.createStructField("location", DataTypes.StringType, true),
                    DataTypes.createStructField("hashtag", DataTypes.StringType, true)
            };
            DataFrame df1 = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "user_location_clean.csv").coalesce(numPart);
            //tweet hashtagGrouped location
            sqlContext.createDataFrame(tweet_user_hashtag_grouped.join(df1, tweet_user_hashtag_grouped.col("username").equalTo(df1.col("C0"))).drop(tweet_user_hashtag_grouped.col("username")).drop(df1.col("C0")).javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getLong(0), v1.getString(2), v1.getString(1));
                }
            }), new StructType(fieldsLocation)).write().parquet(dataPath + "tweet_location_hashtag_grouped_parquet");

        }
        System.out.println(" HAS READ THE TWEET_HASHTAG ");

    }

    public static DataFrame calcFromToProb(final double tweetNum, DataFrame df, String colName, String probName, String tableName, final int grNum){
        //df = df.join(tweetTime, tweetTime.col("tid").equalTo(df.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        df = sqlContext.createDataFrame(df.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getLong(3) <= timestamp;
            }
        }), df.schema());
        //if(probName.equals("locProb"))
        //    df.drop("time").write().mode(SaveMode.Overwrite).parquet(outputPath + tableName + "_time");
        //TODO This is true when a user is only mentioned once in a tweet
        System.out.println(colName);
        JavaRDD<Row> prob = df.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
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
        });
        StructField[] fields = {
                DataTypes.createStructField(colName, DataTypes.StringType, true),
                DataTypes.createStructField(probName, DataTypes.DoubleType, true),
        };

        return sqlContext.createDataFrame(prob, new StructType(fields));
    }


    public static void calcContainHashtagProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final List<String> hashtagList = milionFeatureLists.get(1);
        tweet_hashtag_hashtag_grouped = sqlContext.read().parquet(dataPath+ classname + "/fold0/Ids/" + "tweet_hashtag_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag){
            //tweet_hashtag_hashtag_grouped = tweet_hashtag_hashtag_grouped.join(tweetTime, tweet_hashtag_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
            tweet_hashtag_hashtag_grouped = sqlContext.createDataFrame(tweet_hashtag_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp);
                }
            }), tweet_hashtag_hashtag_grouped.schema());
        }else{
            tweet_hashtag_hashtag_grouped = sqlContext.createDataFrame(tweet_hashtag_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp && hashtagList.contains(v1.getString(1)));
                }
            }), tweet_hashtag_hashtag_grouped.schema());
        }
//        fromHashtagProb = calcFromToProb(tweetCount, tweet_hashtag_hashtag_grouped, "hashtag1", "fromProb", "tweet_hashtag_hashtag_grouped_parquet");
//        fromHashtagProb.registerTempTable("fromHashtagProb");
    }

    public static void calcFromUserProb(final double tweetNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
//        fromUserProb = calcFromToProb(tweetCount, tweet_user_hashtag_grouped, "username1", "fromProb", "tweet_user_hashtag_grouped_parquet");
//        fromUserProb.registerTempTable("fromUserProb");
    }

    public static void calcContainTermProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final List<String> termList = milionFeatureLists.get(3);
        tweet_term_hashtag_grouped = sqlContext.read().parquet(dataPath+ classname + "/fold0/Ids/" + "tweet_term_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag){
            //tweet_term_hashtag_grouped = tweet_term_hashtag_grouped.join(tweetTime, tweet_term_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
            tweet_term_hashtag_grouped = sqlContext.createDataFrame(tweet_term_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp);
                }
            }), tweet_term_hashtag_grouped.schema());
        }else{
            tweet_term_hashtag_grouped = sqlContext.createDataFrame(tweet_term_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp && termList.contains(v1.getString(1)));
                }
            }), tweet_term_hashtag_grouped.schema());
        }
//        containTermProb = calcFromToProb(tweetCount, tweet_term_hashtag_grouped, "term1", "containTermProb", "tweet_term_hashtag_grouped_parquet");
//        containTermProb.registerTempTable("containTermProb");
    }

    public static void calcToUserProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final List<String> mentionList = milionFeatureLists.get(2);
        tweet_mention_hashtag_grouped = sqlContext.read().parquet(dataPath+ classname + "/fold0/Ids/" + "tweet_mention_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag){
            //tweet_mention_hashtag_grouped = tweet_mention_hashtag_grouped.join(tweetTime, tweet_mention_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
            tweet_mention_hashtag_grouped = sqlContext.createDataFrame(tweet_mention_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp);
                }
            }), tweet_mention_hashtag_grouped.schema());
        }else{
            tweet_mention_hashtag_grouped = sqlContext.createDataFrame(tweet_mention_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp && mentionList.contains(v1.getString(1)));
                }
            }), tweet_mention_hashtag_grouped.schema());
        }
        //toUserProb = calcFromToProb(tweetCount, tweet_mention_hashtag_grouped, "username1", "toProb", "tweet_mention_hashtag_grouped_parquet");
        //toUserProb.registerTempTable("toUserProb");
    }

    public static void calcContainLocationProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final List<String> locationList = milionFeatureLists.get(4);
        tweet_location_hashtag_grouped = sqlContext.read().parquet(dataPath + classname + "/fold0/Ids/" + "tweet_location_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag){
            //tweet_location_hashtag_grouped = tweet_location_hashtag_grouped.join(tweetTime, tweet_location_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
            tweet_location_hashtag_grouped = sqlContext.createDataFrame(tweet_location_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp);
                }
            }), tweet_location_hashtag_grouped.schema());
        }else {
            tweet_location_hashtag_grouped = sqlContext.createDataFrame(tweet_location_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return (v1.getLong(3) <= timestamp && locationList.contains(v1.getString(1)));
                }
            }), tweet_location_hashtag_grouped.schema());
        }
//        containLocationProb = calcFromToProb(tweetCount, tweet_location_hashtag_grouped, "username1", "locProb", "tweet_location_hashtag_grouped_parquet");
//        containLocationProb.registerTempTable("containLocationProb");
    }

    /*
      * groupNum: Hashtag Topic Group Number
      * (tweet_Contain_topical_Hashtag | Mention_user)
    */
    public static void calcTweetCondToUserConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
//        tweet_mention_hashtag_grouped = tweet_mention_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_mention_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweet_mention_hashtag_grouped.cache();
        //==============================================================================================================
        //F(toUser & topical)
        DataFrame probToUserTopical = sqlContext.createDataFrame(calcProb(tweet_mention_hashtag_grouped, groupNum, true, tweetCount), new StructType(fields1));
        probToUserTopical.registerTempTable("probToUserTopicalTable");
        probToUserTopical = probToUserTopical.select(probToUserTopical.col("username"), probToUserTopical.col("prob").alias("ToUserTopicalFreq"));
        System.out.println("ProbToUserTopical Count: " + probToUserTopical.count());
        //==============================================================================================================
        //F(toUser & ~topical)
        DataFrame probToUserNotTopical = sqlContext.createDataFrame(calcProb(tweet_mention_hashtag_grouped, groupNum, false, tweetCount), new StructType(fields1));
        probToUserNotTopical.registerTempTable("probToUserNotTopicalTable");
        probToUserNotTopical = probToUserNotTopical.select(probToUserNotTopical.col("username"), probToUserNotTopical.col("prob").alias("ToUserNotTopicalFreq"));
        DataFrame res = probToUserTopical.join(probToUserNotTopical, probToUserTopical.col("username").equalTo(probToUserNotTopical.col("username"))).drop(probToUserNotTopical.col("username"));
        //==============================================================================================================
        //F(~toUser & topical)
        DataFrame probNotToUserTopical = sqlContext.sql("select username, (" + containNotContainCounts[0] + "- prob ) AS prob from probToUserTopicalTable");
        probNotToUserTopical = probNotToUserTopical.select(probNotToUserTopical.col("username").alias("username2"), probNotToUserTopical.col("prob").alias("notToUserTopicalFreq"));
        res = res.join(probNotToUserTopical, res.col("username").equalTo(probNotToUserTopical.col("username2"))).drop(probNotToUserTopical.col("username2"));
        //==============================================================================================================
        //F(~toUser & ~topical)
        DataFrame probNotToUserNotTopical = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - prob ) AS prob from probToUserNotTopicalTable");
        probNotToUserNotTopical = probNotToUserNotTopical.select(probNotToUserNotTopical.col("username").alias("username3"), probNotToUserNotTopical.col("prob").alias("notToUserNotTopicalFreq"));
        res = res.join(probNotToUserNotTopical, res.col("username").equalTo(probNotToUserNotTopical.col("username3"))).drop(probNotToUserNotTopical.col("username3"));

        res.printSchema();
        // username, F(toUser & topical), F(toUser & ~topical), F(~toUser & topical), F(~toUser & ~topical)

        DataFrame res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda1) * (v1.getDouble(4) + lambda1)) / ((v1.getDouble(2) + lambda1) * (v1.getDouble(3) + lambda1))));
            }
        }),new StructType(fields1) );
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser_"+lambda1, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda2) * (v1.getDouble(4) + lambda2)) / ((v1.getDouble(2) + lambda2) * (v1.getDouble(3) + lambda2))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser_"+lambda2, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda3) * (v1.getDouble(4) + lambda3)) / ((v1.getDouble(2) + lambda3) * (v1.getDouble(3) + lambda3))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser_"+lambda3, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda4) * (v1.getDouble(4) + lambda4)) / ((v1.getDouble(2) + lambda4) * (v1.getDouble(3) + lambda4))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser_"+lambda4, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda5) * (v1.getDouble(4) + lambda5)) / ((v1.getDouble(2) + lambda5) * (v1.getDouble(3) + lambda5))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser_"+lambda5, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda6) * (v1.getDouble(4) + lambda6)) / ((v1.getDouble(2) + lambda6) * (v1.getDouble(3) + lambda6))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser_"+lambda6, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0),v1.getDouble(1));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/Rocchio/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/toUser", false);
    }


    public static void calcTweetCondContainLocationConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
//        tweet_location_hashtag_grouped = tweet_location_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_location_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweet_location_hashtag_grouped.cache();
        //==============================================================================================================
        DataFrame probFromLocationTopical = sqlContext.createDataFrame(calcProb(tweet_location_hashtag_grouped, groupNum, true, tweetCount), new StructType(fields1));
        probFromLocationTopical.registerTempTable("probFromLocationTopicalTable");
        probFromLocationTopical = probFromLocationTopical.select(probFromLocationTopical.col("username"), probFromLocationTopical.col("prob").alias("FromLocationTopicalFreq"));
        //==============================================================================================================
        DataFrame probFromLocationNotTopical = sqlContext.createDataFrame(calcProb(tweet_location_hashtag_grouped, groupNum, false, tweetCount), new StructType(fields1));
        probFromLocationNotTopical.registerTempTable("probFromLocationNotTopicalTable");
        probFromLocationNotTopical = probFromLocationNotTopical.select(probFromLocationNotTopical.col("username"), probFromLocationNotTopical.col("prob").alias("fromLocationNotTopicalFreq"));
        DataFrame res = probFromLocationTopical.join(probFromLocationNotTopical, probFromLocationTopical.col("username").equalTo(probFromLocationNotTopical.col("username"))).drop(probFromLocationNotTopical.col("username"));
        //==============================================================================================================
        //F(~toUser & topical)
        DataFrame probNotFromLocationTopical = sqlContext.sql("select username, (" + containNotContainCounts[0] + "- prob ) AS prob from probFromLocationTopicalTable");
        probNotFromLocationTopical = probNotFromLocationTopical.select(probNotFromLocationTopical.col("username").alias("username2"), probNotFromLocationTopical.col("prob").alias("notFromLocationTopicalFreq"));
        res = res.join(probNotFromLocationTopical, res.col("username").equalTo(probNotFromLocationTopical.col("username2"))).drop(probNotFromLocationTopical.col("username2"));
        //==============================================================================================================
        //F(~toUser & ~topical)
        DataFrame probNotFromLocationNotTopical = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - prob ) AS prob from probFromLocationNotTopicalTable");
        probNotFromLocationNotTopical = probNotFromLocationNotTopical.select(probNotFromLocationNotTopical.col("username").alias("username3"), probNotFromLocationNotTopical.col("prob").alias("notFromLocationNotTopicalFreq"));
        res = res.join(probNotFromLocationNotTopical, res.col("username").equalTo(probNotFromLocationNotTopical.col("username3"))).drop(probNotFromLocationNotTopical.col("username3"));

        res.printSchema();
        // username, F(toUser & topical), F(toUser & ~topical), F(~toUser & topical), F(~toUser & ~topical)

        DataFrame res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda1) * (v1.getDouble(4) + lambda1)) / ((v1.getDouble(2) + lambda1) * (v1.getDouble(3) + lambda1))));
            }
        }),new StructType(fields1) );
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation_"+lambda1, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda2) * (v1.getDouble(4) + lambda2)) / ((v1.getDouble(2) + lambda2) * (v1.getDouble(3) + lambda2))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation_"+lambda2, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda3) * (v1.getDouble(4) + lambda3)) / ((v1.getDouble(2) + lambda3) * (v1.getDouble(3) + lambda3))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation_"+lambda3, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda4) * (v1.getDouble(4) + lambda4)) / ((v1.getDouble(2) + lambda4) * (v1.getDouble(3) + lambda4))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation_"+lambda4, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda5) * (v1.getDouble(4) + lambda5)) / ((v1.getDouble(2) + lambda5) * (v1.getDouble(3) + lambda5))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation_"+lambda5, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda6) * (v1.getDouble(4) + lambda6)) / ((v1.getDouble(2) + lambda6) * (v1.getDouble(3) + lambda6))));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation_"+lambda6, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0),v1.getDouble(1));
            }
        }), new StructType(fields1));
        output(res1, "LearningMethods/Rocchio/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromLocation", false);
    }

    /*
      * groupNum: Hashtag Topic Group Number
      * (tweet_Contain_topical_Hashtag | FromUser)
      *
    */
    public static void calcTweetCondFromUserConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
        //==============================================================================================================
        DataFrame probFromUserTopical = sqlContext.createDataFrame(calcProb(tweet_user_hashtag_grouped, groupNum, true, tweetCount), new StructType(fields));
        probFromUserTopical.registerTempTable("probFromUserTopicalTable");
        probFromUserTopical = probFromUserTopical.select(probFromUserTopical.col("username"), probFromUserTopical.col("prob").alias("fromUserTopicalFreq"));
        //==============================================================================================================
        DataFrame probFromUserNotTopical = sqlContext.createDataFrame(calcProb(tweet_user_hashtag_grouped, groupNum, false, tweetCount), new StructType(fields));
        probFromUserNotTopical.registerTempTable("probFromUserNotTopicalTable");
        probFromUserNotTopical = probFromUserNotTopical.select(probFromUserNotTopical.col("username"), probFromUserNotTopical.col("prob").alias("fromUserNotTopicalFreq"));
        DataFrame res = probFromUserTopical.join(probFromUserNotTopical, probFromUserTopical.col("username").equalTo(probFromUserNotTopical.col("username"))).drop(probFromUserNotTopical.col("username"));
        //==============================================================================================================
        //F(~toUser & topical)
        DataFrame probNotFromUserTopical = sqlContext.sql("select username, (" + containNotContainCounts[0] + "- prob ) AS prob from probFromUserTopicalTable");
        probNotFromUserTopical = probNotFromUserTopical.select(probNotFromUserTopical.col("username").alias("username2"), probNotFromUserTopical.col("prob").alias("notFromUserTopicalFreq"));
        res = res.join(probNotFromUserTopical, res.col("username").equalTo(probNotFromUserTopical.col("username2"))).drop(probNotFromUserTopical.col("username2"));
        //==============================================================================================================
        //F(~toUser & ~topical)
        DataFrame probNotFromUserNotTopical = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - prob ) AS prob from probFromUserNotTopicalTable");
        probNotFromUserNotTopical = probNotFromUserNotTopical.select(probNotFromUserNotTopical.col("username").alias("username3"), probNotFromUserNotTopical.col("prob").alias("notFromUserNotTopicalFreq"));
        res = res.join(probNotFromUserNotTopical, res.col("username").equalTo(probNotFromUserNotTopical.col("username3"))).drop(probNotFromUserNotTopical.col("username3"));

        res.printSchema();
        // username, F(toUser & topical), F(toUser & ~topical), F(~toUser & topical), F(~toUser & ~topical)

        DataFrame res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda1) * (v1.getDouble(4) + lambda1)) / ((v1.getDouble(2) + lambda1) * (v1.getDouble(3) + lambda1))));
            }
        }),new StructType(fields) );
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser_"+lambda1, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda2) * (v1.getDouble(4) + lambda2)) / ((v1.getDouble(2) + lambda2) * (v1.getDouble(3) + lambda2))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser_"+lambda2, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda3) * (v1.getDouble(4) + lambda3)) / ((v1.getDouble(2) + lambda3) * (v1.getDouble(3) + lambda3))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser_"+lambda3, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda4) * (v1.getDouble(4) + lambda4)) / ((v1.getDouble(2) + lambda4) * (v1.getDouble(3) + lambda4))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser_"+lambda4, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda5) * (v1.getDouble(4) + lambda5)) / ((v1.getDouble(2) + lambda5) * (v1.getDouble(3) + lambda5))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser_"+lambda5, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda6) * (v1.getDouble(4) + lambda6)) / ((v1.getDouble(2) + lambda6) * (v1.getDouble(3) + lambda6))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser_"+lambda6, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0),v1.getDouble(1));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/Rocchio/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/fromUser", false);
    }


    private static void output(DataFrame data, String folderName, boolean flag) {
        if(flag)
            data.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/" + folderName + "_csv");
        //data.write().mode(SaveMode.Overwrite).parquet(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/"+ folderName + "_parquet");
        data.coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath+folderName + "_parquet");
    }

    private static JavaRDD<Row> calcProb(DataFrame df, final int groupNum, final boolean containFlag, final double tweetNum) throws IOException {
        final HashSet<String> hashtagList = allHashtags; //tweetUtil.getTestTrainGroupHashtagList(groupNum, localRun, true);
        final String delim = (testFlag)? "," : " ";
        return df.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                int numHashtags= 0;
                if(row.get(2) != null) {
                    List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(2).split(delim))));
                    tH.retainAll(hashtagList);
                    numHashtags = tH.size();
                }
                if (containFlag) {
                    if (numHashtags > 0)
                        return new Tuple2<String, Double>(row.getString(1), 1.0);
                    else
                        return new Tuple2<String, Double>(row.getString(1), 0.0);
                } else {
                    if (numHashtags == 0)
                        return new Tuple2<String, Double>(row.getString(1), 1.0);
                    else
                        return new Tuple2<String, Double>(row.getString(1), 0.0);
                }
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
        });
    }

    public static long[] getContainNotContainCounts(final int groupNum) throws IOException {
        final HashSet<String> hashtagList = allHashtags;
        long[] counts = new long[2];
        final String delim = (testFlag)? "," : " ";
        JavaRDD<Row> containNotContainNum = tweet_user_hashtag_grouped.drop("username").coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(Row row) throws Exception {
                if(row.get(1) == null)
                    return new Tuple2<Integer, Long>(2, 1l);
                List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(1).split(delim))));
                tH.retainAll(hashtagList);
                if (tH.size() > 0)
                    return new Tuple2<Integer, Long>(1, 1l);
                else
                    return new Tuple2<Integer, Long>(2, 1l);
            }
        }).reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong + aLong2;
            }
        }).map(new Function<Tuple2<Integer, Long>, Row>() {
            @Override
            public Row call(Tuple2<Integer, Long> integerLongTuple2) throws Exception {
                return RowFactory.create(integerLongTuple2._1(), integerLongTuple2._2());
            }
        });
        StructField[] fields1 = {
                DataTypes.createStructField("key", DataTypes.IntegerType, true),
                DataTypes.createStructField("num", DataTypes.LongType, true),
        };
        (sqlContext.createDataFrame(containNotContainNum, new StructType(fields1))).registerTempTable("containNumTable");
        counts[0] = sqlContext.sql("select num from containNumTable where key = 1").head().getLong(0);
        counts[1] = sqlContext.sql("select num from containNumTable where key = 2").head().getLong(0);
        System.out.println("=============== CONTAIN: " + counts[0] + " ============ NOT CONTAIN: " + counts[1]);
        return counts;
    }

     /*
    *
    * (Tweet_contain_topical_hashtag | contain_hashtag)
    *
     */

    public static void calcTweetCondContainHashtagConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields = {
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
//        tweet_hashtag_hashtag_grouped = tweet_hashtag_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_hashtag_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweet_hashtag_hashtag_grouped.cache();
        DataFrame probContainHashtagTopical = sqlContext.createDataFrame(calcProb(tweet_hashtag_hashtag_grouped, groupNum, true, tweetCount), new StructType(fields));
        probContainHashtagTopical.registerTempTable("probContainHashtagTopicalTable");
        probContainHashtagTopical = probContainHashtagTopical.select(probContainHashtagTopical.col("hashtag"), probContainHashtagTopical.col("prob").alias("containHashtagTopicalFreq"));
        //==============================================================================================================
        DataFrame probContainHashtagNotTopical = sqlContext.createDataFrame(calcProb(tweet_hashtag_hashtag_grouped, groupNum, false, tweetCount), new StructType(fields));
        probContainHashtagNotTopical.registerTempTable("probContainHashtagNotTopicalTable");
        probContainHashtagNotTopical = probContainHashtagNotTopical.select(probContainHashtagNotTopical.col("hashtag"), probContainHashtagNotTopical.col("prob").alias("containHashtagNotTopicalFreq"));
        DataFrame res = probContainHashtagTopical.join(probContainHashtagNotTopical, probContainHashtagTopical.col("hashtag").equalTo(probContainHashtagNotTopical.col("hashtag"))).drop(probContainHashtagNotTopical.col("hashtag"));
        //==============================================================================================================
        //F(~toUser & topical)
        DataFrame probNotContainHashtagTopical = sqlContext.sql("select hashtag, (" + containNotContainCounts[0] + "- prob ) AS prob from probContainHashtagTopicalTable");
        probNotContainHashtagTopical = probNotContainHashtagTopical.select(probNotContainHashtagTopical.col("hashtag").alias("hashtag2"), probNotContainHashtagTopical.col("prob").alias("notContainHashtagTopicalFreq"));
        res = res.join(probNotContainHashtagTopical, res.col("hashtag").equalTo(probNotContainHashtagTopical.col("hashtag2"))).drop(probNotContainHashtagTopical.col("hashtag2"));
        //==============================================================================================================
        //F(~toUser & ~topical)
        DataFrame probNotContainHashtagNotTopical = sqlContext.sql("select hashtag, (" + containNotContainCounts[1] + " - prob ) AS prob from probContainHashtagNotTopicalTable");
        probNotContainHashtagNotTopical = probNotContainHashtagNotTopical.select(probNotContainHashtagNotTopical.col("hashtag").alias("hashtag3"), probNotContainHashtagNotTopical.col("prob").alias("notContainHashtagNotTopicalFreq"));
        res = res.join(probNotContainHashtagNotTopical, res.col("hashtag").equalTo(probNotContainHashtagNotTopical.col("hashtag3"))).drop(probNotContainHashtagNotTopical.col("hashtag3"));

        res.printSchema();
        // username, F(toUser & topical), F(toUser & ~topical), F(~toUser & topical), F(~toUser & ~topical)

        DataFrame res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda1) * (v1.getDouble(4) + lambda1)) / ((v1.getDouble(2) + lambda1) * (v1.getDouble(3) + lambda1))));
            }
        }),new StructType(fields) );
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag_"+lambda1, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda2) * (v1.getDouble(4) + lambda2)) / ((v1.getDouble(2) + lambda2) * (v1.getDouble(3) + lambda2))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag_"+lambda2, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda3) * (v1.getDouble(4) + lambda3)) / ((v1.getDouble(2) + lambda3) * (v1.getDouble(3) + lambda3))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag_"+lambda3, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda4) * (v1.getDouble(4) + lambda4)) / ((v1.getDouble(2) + lambda4) * (v1.getDouble(3) + lambda4))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag_"+lambda4, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda5) * (v1.getDouble(4) + lambda5)) / ((v1.getDouble(2) + lambda5) * (v1.getDouble(3) + lambda5))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag_"+lambda5, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda6) * (v1.getDouble(4) + lambda6)) / ((v1.getDouble(2) + lambda6) * (v1.getDouble(3) + lambda6))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag_"+lambda6, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0),v1.getDouble(1));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/Rocchio/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containHashtag", false);
    }

    /*
    *
    * (Tweet_contain_topical_hashtag | contain_term)
    *
     */

    public static void calcTweetCondContainTermConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields = {
                DataTypes.createStructField("term", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
//        tweet_term_hashtag_grouped = tweet_term_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_term_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweet_term_hashtag_grouped.cache();

        DataFrame probContainTermTopical = sqlContext.createDataFrame(calcProb(tweet_term_hashtag_grouped, groupNum, true, tweetCount), new StructType(fields));
        probContainTermTopical.registerTempTable("probContainTermTopicalTable");
        probContainTermTopical = probContainTermTopical.select(probContainTermTopical.col("term"), probContainTermTopical.col("prob").alias("containTermTopicalFreq"));
        //==============================================================================================================
        DataFrame probContainTermNotTopical = sqlContext.createDataFrame(calcProb(tweet_term_hashtag_grouped, groupNum, false, tweetCount), new StructType(fields));
        probContainTermNotTopical.registerTempTable("probContainTermNotTopicalTable");
        probContainTermNotTopical = probContainTermNotTopical.select(probContainTermNotTopical.col("term"), probContainTermNotTopical.col("prob").alias("containTermNotTopicalFreq"));
        DataFrame res = probContainTermTopical.join(probContainTermNotTopical, probContainTermTopical.col("term").equalTo(probContainTermNotTopical.col("term"))).drop(probContainTermNotTopical.col("term"));
        //==============================================================================================================
        //F(~toUser & topical)
        DataFrame probNotContainTermTopical = sqlContext.sql("select term, (" + containNotContainCounts[0] + "- prob ) AS prob from probContainTermTopicalTable");
        probNotContainTermTopical = probNotContainTermTopical.select(probNotContainTermTopical.col("term").alias("term2"), probNotContainTermTopical.col("prob").alias("notContainTermTopicalFreq"));
        res = res.join(probNotContainTermTopical, res.col("term").equalTo(probNotContainTermTopical.col("term2"))).drop(probNotContainTermTopical.col("term2"));
        //==============================================================================================================
        //F(~toUser & ~topical)
        DataFrame probNotContainTermNotTopical = sqlContext.sql("select term, (" + containNotContainCounts[1] + " - prob ) AS prob from probContainTermNotTopicalTable");
        probNotContainTermNotTopical = probNotContainTermNotTopical.select(probNotContainTermNotTopical.col("term").alias("term3"), probNotContainTermNotTopical.col("prob").alias("notContainTermNotTopicalFreq"));
        res = res.join(probNotContainTermNotTopical, res.col("term").equalTo(probNotContainTermNotTopical.col("term3"))).drop(probNotContainTermNotTopical.col("term3"));

        res.printSchema();
        // username, F(toUser & topical), F(toUser & ~topical), F(~toUser & topical), F(~toUser & ~topical)

        DataFrame res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda1) * (v1.getDouble(4) + lambda1)) / ((v1.getDouble(2) + lambda1) * (v1.getDouble(3) + lambda1))));
            }
        }),new StructType(fields) );
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm_"+lambda1, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda2) * (v1.getDouble(4) + lambda2)) / ((v1.getDouble(2) + lambda2) * (v1.getDouble(3) + lambda2))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm_"+lambda2, false);
        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda3) * (v1.getDouble(4) + lambda3)) / ((v1.getDouble(2) + lambda3) * (v1.getDouble(3) + lambda3))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm_"+lambda3, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda4) * (v1.getDouble(4) + lambda4)) / ((v1.getDouble(2) + lambda4) * (v1.getDouble(3) + lambda4))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm_"+lambda4, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda5) * (v1.getDouble(4) + lambda5)) / ((v1.getDouble(2) + lambda5) * (v1.getDouble(3) + lambda5))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm_"+lambda5, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0), Math.log(((v1.getDouble(1) + lambda6) * (v1.getDouble(4) + lambda6)) / ((v1.getDouble(2) + lambda6) * (v1.getDouble(3) + lambda6))));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/NB/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm_"+lambda6, false);

        res1 = sqlContext.createDataFrame(res.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create(v1.getString(0),v1.getDouble(1));
            }
        }), new StructType(fields));
        output(res1, "LearningMethods/Rocchio/fold" + kValue + "/" + testVal+"/" + groupNames[groupNum-1] + "/containTerm", false);
    }

}
