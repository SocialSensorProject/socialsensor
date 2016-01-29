package machinelearning.mutualInformation1;

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
import preprocess.spark.ConfigRead;
import scala.Tuple2;
import util.TweetUtil;

import java.io.IOException;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zahraiman on 8/10/15.
 */
public class ComputeMIForLearning {
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
    private static final boolean writeTopicalLocation = false;
    private static final boolean writeTopicalTerm = false;
    private static final boolean writeTopicalFrom = false;
    private static final boolean writeTopicalHashtag = false;
    private static final boolean writeTopicalMention = false;
    private static int groupNum;
    private static int numOfGroups;
    private static String[] groupNames;
    private static TweetUtil tweetUtil = new TweetUtil();
    //private static DataFrame tweetTime;
    private static final int topFeatureNum = 1000;
    private static boolean testFlag;
    private static final double lambda1 = 0.001;
    private static final double lambda2 = 0.01;
    private static final double lambda3 = 0.1;
    private static boolean computeTweetLocation = false;
    private static List<List<String>> milionFeatureLists;

    private static final long[] timestamps= {1377897403000l, 1362146018000l, 1391295058000l, 1372004539000l, 1359920993000l, 1364938764000l, 1378911100000l, 1360622109000l, 1372080004000l, 1360106035000l};;


    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String[] args) throws IOException, ParseException, InterruptedException {
        loadConfig();
        String inputPath = "/data/ClusterData/input/";
        testFlag = configRead.getTestFlag();
        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        if(testFlag){
            timestamps[0] = format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime();
        }

        //long t1 = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse("Fri Feb 28 11:00:00 +0000 2014").getTime();
        numOfGroups = configRead.getNumOfGroups();
        groupNames = configRead.getGroupNames();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();

        outputPath = hdfsPath + configRead.getOutputPath();
        localRun = configRead.isLocal();
        topUserNum = configRead.getTopUserNum();
        TweetUtil tweetUtil = new TweetUtil();
        milionFeatureLists = tweetUtil.get1MFeatures(localRun);

        for(groupNum = 1; groupNum <= 1; groupNum++) {
            dataPath = "/data/ClusterData/input/Data/Learning/Topics/" + groupNames[groupNum-1] + "/fold0/Ids/";
            if(testFlag) {
                dataPath = "TestSet/Data/";
                outputPath = "TestSet/Out/";
            }
            if(groupNum > 1 && groupNum != 7)
                continue;
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

            DataFrame df1 = sqlContext.read().parquet(outputPath + "TopMIfeatures/" + groupNames[groupNum - 1] + "/top_Term_parquet");
            df1 = df1.unionAll(sqlContext.read().parquet(outputPath + "TopMIfeatures/" + groupNames[groupNum - 1] + "/top_mention_parquet"));
            df1 = df1.unionAll(sqlContext.read().parquet(outputPath + "TopMIfeatures/" + groupNames[groupNum - 1] + "/top_location_parquet"));
            df1 = df1.unionAll(sqlContext.read().parquet(outputPath + "TopMIfeatures/" + groupNames[groupNum - 1] + "/top_hashtag_parquet"));
            df1 = df1.unionAll(sqlContext.read().parquet(outputPath + "TopMIfeatures/" + groupNames[groupNum - 1] + "/top_from_parquet"));
            df1 = df1.sort(df1.col("mutualEntropy").desc());
//            df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "TopMIfeatures/Topics/"+groupNames[groupNum - 1] + "/featuresMI_parquet");
            df1.coalesce(1).write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "TopMIfeatures/Topics/" + groupNames[groupNum - 1] + "/featuresMI_parquet");
            tweetUtil.runStringCommand("mv " + outputPath + "TopMIfeatures/Topics/" + groupNames[groupNum - 1] + "/featuresMI_parquet" + "/part-00000 " + outputPath + "TopMIfeatures/Topics/" + groupNames[groupNum - 1] + "/featuresMI.csv");
        }
    }

    public static void initializeSqlContext() throws IOException {
        SparkConf sparkConfig;
        if(localRun) {
//            dataPath = configRead.getTestDataPath();
//            dataPath = "TestSet/data1Month/";
//            outputPath = configRead.getTestOutPath();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[8]").set("spark.executor.memory", "12g").set("spark.driver.maxResultSize", "6g");
            tweetCount = 100;
//            if(testFlag)
//                tweetCount = 2000;
        }else {
//            tweetCount = 829026458; //tweet_user.count();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[8]");
        }
        sparkContext = new JavaSparkContext(sparkConfig);
        sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);
        System.out.println("======================GROUPNUM: " + groupNum + dataPath + "tweet_time_parquet");
        final int grNum = groupNum;
        if(testFlag) {
            tweetTime = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);
            StructField[] timeField = {
                    DataTypes.createStructField("tid", DataTypes.LongType, true),
                    DataTypes.createStructField("time", DataTypes.LongType, true)
            };
            tweetTime = sqlContext.createDataFrame(tweetTime.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getLong(1) <= timestamps[grNum - 1];
                }
            }), new StructType(timeField)).coalesce(numPart);
            tweetTime.cache();
            //tweetCount = tweetTime.count();
        }
        //System.out.println("=================================Tweet Time Size: " + tweetCount);
        System.out.println(dataPath + "tweet_user_hashtag_grouped_parquet");
        tweet_user_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);

        if(testFlag)
            tweet_user_hashtag_grouped = tweet_user_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_user_hashtag_grouped.col("tid")), "inner").drop(tweetTime.col("tid")).coalesce(numPart);
        tweet_user_hashtag_grouped = sqlContext.createDataFrame(tweet_user_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return (v1.getLong(3) <= timestamps[grNum - 1]);
            }
        }), tweet_user_hashtag_grouped.schema());
        tweetCount = tweet_user_hashtag_grouped.count();
        System.out.println(" \n Number of tweets in train data ("+timestamps[grNum-1]+") : " + tweet_user_hashtag_grouped.count());
        //Compute topical and notTopical counts before filtering tweet_user_hashtag table
        containNotContainCounts = getContainNotContainCounts(groupNum);

        final List<String> fromList = milionFeatureLists.get(0);
        tweet_user_hashtag_grouped = sqlContext.createDataFrame(tweet_user_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return fromList.contains(v1.getString(1));
            }
        }), tweet_user_hashtag_grouped.schema()).coalesce(numPart);
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
                return v1.getLong(3) <= timestamps[grNum - 1];
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

        System.out.println(dataPath + "tweet_hashtag_hashtag_grouped_parquet *****************");
        tweet_hashtag_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag)
            tweet_hashtag_hashtag_grouped = tweet_hashtag_hashtag_grouped.join(tweetTime, tweet_hashtag_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
        tweet_hashtag_hashtag_grouped = sqlContext.createDataFrame(tweet_hashtag_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return (v1.getLong(3) <= timestamps[grNum-1] && hashtagList.contains(v1.getString(1)));
            }
        }), tweet_hashtag_hashtag_grouped.schema()).coalesce(numPart);

        fromHashtagProb = calcFromToProb(tweetCount, tweet_hashtag_hashtag_grouped, "hashtag1", "fromProb", "tweet_hashtag_hashtag_grouped_parquet");
        fromHashtagProb.registerTempTable("fromHashtagProb");
        if(writeTopicalHashtag) {
            DataFrame df = fromHashtagProb.sort(fromHashtagProb.col("fromProb").desc()).limit(topFeatureNum).coalesce(numPart);
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topical/top1000_Hashtag");
            sqlContext.createDataFrame(df.javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0), Math.log(v1.getDouble(1)));
                }
            }), new StructType(fields1)).coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topicalLog/top1000_Hashtag");
            ;
            fromHashtagProb.cache();
        }
    }

    public static void calcFromUserProb(final double tweetNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        fromUserProb = calcFromToProb(tweetCount, tweet_user_hashtag_grouped, "username1", "fromProb", "tweet_user_hashtag_grouped_parquet");
        fromUserProb.registerTempTable("fromUserProb");
        if(writeTopicalFrom) {
            DataFrame df = fromUserProb.sort(fromUserProb.col("fromProb").desc()).limit(topFeatureNum).coalesce(numPart);
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topical/top1000_From");
            sqlContext.createDataFrame(df.javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0), Math.log(v1.getDouble(1)));
                }
            }), new StructType(fields1)).coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topicalLog/top1000_From");
            fromUserProb.cache();
        }
    }

    public static void calcContainTermProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };

        final List<String> termList = milionFeatureLists.get(3);
        tweet_term_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag)
            tweet_term_hashtag_grouped = tweet_term_hashtag_grouped.join(tweetTime, tweet_term_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
        tweet_term_hashtag_grouped = sqlContext.createDataFrame(tweet_term_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return (v1.getLong(3) <= timestamps[grNum-1] && termList.contains(v1.getString(1)));
            }
        }), tweet_term_hashtag_grouped.schema()).coalesce(numPart);
        containTermProb = calcFromToProb(tweetCount, tweet_term_hashtag_grouped, "term1", "containTermProb", "tweet_term_hashtag_grouped_parquet");
        containTermProb.registerTempTable("containTermProb");
        if(writeTopicalTerm) {
            DataFrame df = containTermProb.sort(containTermProb.col("containTermProb").desc()).limit(topFeatureNum).coalesce(numPart);
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topical/top1000_Term");
            sqlContext.createDataFrame(df.javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0), Math.log(v1.getDouble(1)));
                }
            }), new StructType(fields1)).coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topicalLog/top1000_Term");
            ;
            containTermProb.cache();
        }
    }

    public static void calcToUserProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final List<String> mentionList = milionFeatureLists.get(2);

        tweet_mention_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag)
            tweet_mention_hashtag_grouped = tweet_mention_hashtag_grouped.join(tweetTime, tweet_mention_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
        tweet_mention_hashtag_grouped = sqlContext.createDataFrame(tweet_mention_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return (v1.getLong(3) <= timestamps[grNum - 1] && mentionList.contains(v1.getString(1)));
            }
        }), tweet_mention_hashtag_grouped.schema()).coalesce(numPart);
        toUserProb = calcFromToProb(tweetCount, tweet_mention_hashtag_grouped, "username1", "toProb", "tweet_mention_hashtag_grouped_parquet");
        toUserProb.registerTempTable("toUserProb");
        if(writeTopicalMention) {
            DataFrame df = toUserProb.sort(toUserProb.col("toProb").desc()).limit(topFeatureNum).coalesce(numPart);
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topical/top1000_Mention");
            sqlContext.createDataFrame(df.javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0), Math.log(v1.getDouble(1)));
                }
            }), new StructType(fields1)).coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topicalLog/top1000_Mention");
            toUserProb.cache();
        }
    }

    public static void calcContainLocationProb(final double tweetNum, final int grNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final List<String> locationList = milionFeatureLists.get(4);

        tweet_location_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_location_hashtag_grouped_parquet").coalesce(numPart);
        if(testFlag)
            tweet_location_hashtag_grouped = tweet_location_hashtag_grouped.join(tweetTime, tweet_location_hashtag_grouped.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid"));
        tweet_location_hashtag_grouped = sqlContext.createDataFrame(tweet_location_hashtag_grouped.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return (v1.getLong(3) <= timestamps[grNum-1] && locationList.contains(v1.getString(1)));
            }
        }), tweet_location_hashtag_grouped.schema()).coalesce(numPart);
        containLocationProb = calcFromToProb(tweetCount, tweet_location_hashtag_grouped, "username1", "locProb", "tweet_location_hashtag_grouped_parquet");
        containLocationProb.registerTempTable("containLocationProb");
        if(writeTopicalLocation) {
            DataFrame df = containLocationProb.sort(containLocationProb.col("locProb").desc()).limit(topFeatureNum).coalesce(numPart);
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topical/top1000_Location");
            sqlContext.createDataFrame(df.javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0), Math.log(v1.getDouble(1)));
                }
            }), new StructType(fields1)).coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/topicalLog/top1000_Location");
            containLocationProb.cache();
        }
    }

    public static DataFrame calcFromToProb(final double tweetNum, DataFrame df, String colName, String probName, String tableName){
//        df = df.join(tweetTime, tweetTime.col("tid").equalTo(df.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
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

    public static void calcTweetCondToUserConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final double probTweetContain = (double) containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double) containNotContainCounts[1] / tweetCount;
        tweet_mention_hashtag_grouped.printSchema();
        //System.out.println("LOOOOOOOOK-ToUSER: " + tweet_mention_hashtag_grouped.count());
        //==============================================================================================================
        JavaRDD<Row> probContainTweet = calcProb(tweet_mention_hashtag_grouped, groupNum, true, tweetCount);
        DataFrame results2 = sqlContext.createDataFrame(probContainTweet.coalesce(numPart), new StructType(fields1));
        results2.registerTempTable("condEntropyTweetTrueToUserTrue");
        DataFrame toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = true|  ToUser = true)
        JavaRDD<Row> toresMI = toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (row.getDouble(2)/tweetNum))));
            }
        }).coalesce(numPart);

        //System.out.println("SIZE 1 TO =================" + toresMI.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet =
                calcProb(tweet_mention_hashtag_grouped, groupNum, false, tweetCount);
        results2 = (sqlContext.createDataFrame(probNotContainTweet.coalesce(numPart).distinct(), new StructType(fields1)));
        results2.registerTempTable("condEntropyTweetFalseToUserTrue");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = False|  ToUser = true)
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (row.getDouble(2) / tweetNum))));
            }
        })).coalesce(numPart);

        //==============================================================================================================
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[0] + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueToUserTrue");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - (row.getDouble(2) / tweetNum)))));
            }
        })).coalesce(numPart);
        (sqlContext.createDataFrame(toresMI, new StructType(fields1))).show();
        //==============================================================================================================
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseToUserTrue");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - (row.getDouble(2) / tweetNum)))));
            }
        })).coalesce(numPart);
        sqlContext.createDataFrame(toresMI, new StructType(fields1)).coalesce(numPart).registerTempTable("mutualEntropyTweetToUser");
        sqlContext.createDataFrame(toresMI, new StructType(fields1)).show();
        StructField[] resFields = {
                DataTypes.createStructField("feature", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mutualEntropy", DataTypes.DoubleType, true)
        };
        toresults2 = sqlContext.createDataFrame(sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetToUser GROUP BY username").javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create("mention", v1.getString(0), v1.getDouble(1));
            }
        }), new StructType(resFields));
        toresults2 = toresults2.sort(toresults2.col("mutualEntropy").desc());//.limit(topFeatureNum).coalesce(numPart);
        output(toresults2, "TopMIfeatures/" + groupNames[groupNum-1] + "/top_Mention", false);
    }


    public static void calcTweetCondContainLocationConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final double probTweetContain = (double) containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double) containNotContainCounts[1] / tweetCount;
        tweet_location_hashtag_grouped.cache();
        //System.out.println("LOOOOOOOOK-Location: " + tweet_location_hashtag_grouped.count());
        //==============================================================================================================
        JavaRDD<Row> probContainTweet = calcProb(tweet_location_hashtag_grouped, groupNum, true, tweetCount);
        DataFrame results2 = sqlContext.createDataFrame(probContainTweet.coalesce(numPart), new StructType(fields1));
        results2.registerTempTable("condEntropyTweetTrueContainLocationTrue");
        DataFrame toresults2 = results2.join(containLocationProb, containLocationProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = true|  ContainLocation = true)
        JavaRDD<Row> toresMI = toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (row.getDouble(2)/tweetNum))));
            }
        }).coalesce(numPart);
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet = calcProb(tweet_location_hashtag_grouped, groupNum, false, tweetCount);
        results2 = (sqlContext.createDataFrame(probNotContainTweet.coalesce(numPart).distinct(), new StructType(fields1)));
        results2.registerTempTable("condEntropyTweetFalseContainLocationTrue");
        toresults2 = results2.join(containLocationProb, containLocationProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = False|  ContainLocation = true)
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (row.getDouble(2)/tweetNum))));
            }
        })).coalesce(numPart);
        //==============================================================================================================
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[0] + "-(prob*" + BigInteger.valueOf((long) tweetCount)
                + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueContainLocationTrue");
        toresults2 = results2.join(containLocationProb, containLocationProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - (row.getDouble(2) / tweetNum)))));
            }
        })).coalesce(numPart);
        //==============================================================================================================
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseContainLocationTrue");
        toresults2 = results2.join(containLocationProb, containLocationProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - (row.getDouble(2) / tweetNum)))));
            }
        })).coalesce(numPart);
        sqlContext.createDataFrame(toresMI, new StructType(fields1)).coalesce(numPart).registerTempTable("mutualEntropyTweetContainLocation");


        StructField[] resFields = {
                DataTypes.createStructField("feature", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mutualEntropy", DataTypes.DoubleType, true)
        };
        toresults2 = sqlContext.createDataFrame(sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetContainLocation GROUP BY username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create("location", v1.getString(0), v1.getDouble(1));
            }
        }), new StructType(resFields));
        toresults2 = toresults2.sort(toresults2.col("mutualEntropy").desc());//.limit(topFeatureNum).coalesce(numPart);
        output(toresults2, "TopMIfeatures/" + groupNames[groupNum-1] + "/top_Location", false);

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
        StructField[] countFields = {
                DataTypes.createStructField("username1", DataTypes.StringType, true),
                DataTypes.createStructField("count", DataTypes.DoubleType, true),
        };

        //==============================================================================================================
        final double probTweetContain = (double)containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double)containNotContainCounts[1] / tweetCount;

        JavaRDD<Row> probContainTweet = calcProb(tweet_user_hashtag_grouped, groupNum, true, tweetCount);
        //System.out.println("LOOOOOOOOK-FromUSER: " + tweet_user_hashtag_grouped.count());
        DataFrame results2 = sqlContext.createDataFrame(probContainTweet.coalesce(numPart), new StructType(fields));
        results2.registerTempTable("condEntropyTweetTrueFromUserTrue");
        //CE_P(Y = true|  FromUser = true)
        DataFrame fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = true|  FromUser = true)
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (row.getDouble(2)/tweetNum))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable1");
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet = calcProb(tweet_user_hashtag_grouped, groupNum, false, tweetCount);
        results2 = (sqlContext.createDataFrame(probNotContainTweet.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = False|  FromUser = true)
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (row.getDouble(2)/tweetNum))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable2");
        DataFrame resMI = sqlContext.sql("SELECT mt1.username, (mt1.prob+mt2.prob) AS prob FROM MITable1 mt1, MITable2 mt2 where mt1.username = mt2.username");
        resMI.registerTempTable("MITable3");
        //==============================================================================================================
        //calculate condEntropyTweetTrueFromUserFalse
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[0] + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - (row.getDouble(2)/tweetNum)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable4");
        resMI = sqlContext.sql("SELECT mt1.username, (mt1.prob+mt2.prob) AS prob FROM MITable3 mt1, MITable4 mt2 where mt1.username = mt2.username");
        resMI.registerTempTable("MITable5");
        //==============================================================================================================
        //calculate condEntropyTweetFalseFromUserFalse
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - (row.getDouble(2)/tweetNum)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable6");

        StructField[] resFields = {
                DataTypes.createStructField("feature", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mutualEntropy", DataTypes.DoubleType, true)
        };
        resMI = sqlContext.createDataFrame(sqlContext.sql("SELECT mt1.username as username, (mt1.prob+mt2.prob) AS mutualEntropy FROM MITable5 mt1, MITable6 mt2 where mt1.username = mt2.username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create("from", v1.getString(0), v1.getDouble(1));
            }
        }), new StructType(resFields));
        resMI = resMI.sort(resMI.col("mutualEntropy").desc());//.limit(topFeatureNum).coalesce(numPart);
        output(resMI, "TopMIfeatures/" + groupNames[groupNum-1] + "/top_From", false);
    }


    private static void output(DataFrame data, String folderName, boolean flag) {
        if(flag)
            data.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/" + folderName + "_csv");
        //data.write().mode(SaveMode.Overwrite).parquet(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/"+ folderName + "_parquet");
        data.coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath+folderName + "_parquet");
    }

    private static JavaRDD<Row> calcProb(DataFrame df, final int groupNum, final boolean containFlag, final double tweetNum) throws IOException {
        final List<String> hashtagList = tweetUtil.getTestTrainGroupHashtagList(groupNum, localRun, true);
        final boolean testFlag2 = testFlag;
        return df.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                int numHashtags= 0;
                if(row.get(2) != null) {
                    String delim = (testFlag2)? "," : " ";
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
                if(tweetNum == 0)
                    System.err.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2()/tweetNum);
            }
        });
    }

    public static long[] getContainNotContainCounts(final int groupNum) throws IOException {
        long[] counts = new long[2];
        final List<String> trainHashtagList =  tweetUtil.getTestTrainGroupHashtagList(groupNum, localRun, true);
        final String delim = (testFlag)? ",": " ";
        JavaRDD<Row> containNotContainNum = tweet_user_hashtag_grouped.drop("username").distinct().coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(Row row) throws Exception {
                if(row.get(1) == null)
                    return new Tuple2<Integer, Long>(2, 1l);
                List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(1).split(delim))));
                tH.retainAll(trainHashtagList);
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
        tweet_hashtag_hashtag_grouped.cache();
        //System.out.println("LOOOOOOOOK-Hashtag: " + tweet_hashtag_hashtag_grouped.count());
        //==============================================================================================================

        final double probTweetContain = (double)containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double)containNotContainCounts[1] / tweetCount;

        JavaRDD<Row> probContainTweet = calcProb(tweet_hashtag_hashtag_grouped, groupNum, true, tweetCount);
        DataFrame results2 = sqlContext.createDataFrame(probContainTweet.coalesce(numPart), new StructType(fields));
        results2.registerTempTable("condEntropyTweetTrueContainHashtagTrue");
        DataFrame fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        JavaRDD<Row> resMI = fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (row.getDouble(2)/tweetNum))));
            }
        });
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet = calcProb(tweet_hashtag_hashtag_grouped, groupNum, false, tweetCount);
        results2 = (sqlContext.createDataFrame(probNotContainTweet.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseContainHashtagTrue");
        fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (row.getDouble(2) / tweetNum))));
            }
        }));
        //==============================================================================================================
        results2 = sqlContext.sql("select hashtag, (" + containNotContainCounts[0] + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueContainHashtagTrue");
        fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - (row.getDouble(2) / tweetNum)))));
            }
        }));
        //==============================================================================================================
        results2 = sqlContext.sql("select hashtag, (" + containNotContainCounts[1] + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseContainHashtagTrue");
        fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - (row.getDouble(2) / tweetNum)))));
            }
        }));
        //System.out.println("SIZE 4 Hashtag=================" + resMI.count() + "================");
        sqlContext.createDataFrame(resMI.coalesce(numPart), new StructType(fields)).registerTempTable("mutualEntropyTweetContainHashtag");

        //======================== COMPUTE COND ENTROPY=================================================================
        StructField[] resFields = {
                DataTypes.createStructField("feature", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mutualEntropy", DataTypes.DoubleType, true)
        };
        fromresults2 = sqlContext.createDataFrame(sqlContext.sql("SELECT hashtag, sum(prob) AS mutualEntropy FROM mutualEntropyTweetContainHashtag GROUP BY hashtag").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create("hashtag", v1.getString(0), v1.getDouble(1));
            }
        }), new StructType(resFields));
        fromresults2 = fromresults2.sort(fromresults2.col("mutualEntropy").desc());//.limit(topFeatureNum).coalesce(numPart);
        output(fromresults2, "TopMIfeatures/" + groupNames[groupNum-1] + "/top_Hashtag", false);


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
        tweet_term_hashtag_grouped.cache();
        //System.out.println("LOOOOOOOOK-Term: " + tweet_term_hashtag_grouped.count());
        //==============================================================================================================
        final double probTweetContain = (double)containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double)containNotContainCounts[1] / tweetCount;

        JavaRDD<Row> probContainTweet = calcProb(tweet_term_hashtag_grouped, groupNum, true, tweetCount);
        DataFrame results2 = sqlContext.createDataFrame(probContainTweet.coalesce(numPart), new StructType(fields));
        results2.registerTempTable("condEntropyTweetTrueContainTermTrue");
        DataFrame fromresults2 = results2.join(containTermProb, containTermProb.col("term1").equalTo(results2.col("term"))).drop("term1");
        //MI_P(Y = true|  ContainTerm = true)
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (row.getDouble(2)/tweetNum))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable1");
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet = calcProb(tweet_term_hashtag_grouped, groupNum, false, tweetCount);
        results2 = (sqlContext.createDataFrame(probNotContainTweet.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseContainTermTrue");
        fromresults2 = results2.join(containTermProb, containTermProb.col("term1").equalTo(results2.col("term"))).drop("term1");
        //MI_P(Y = False|  ContainTerm = true)
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (row.getDouble(2)/tweetNum))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable2");
        DataFrame resMI = sqlContext.sql("SELECT mt1.term, (mt1.prob+mt2.prob) AS prob FROM MITable1 mt1, MITable2 mt2 where mt1.term = mt2.term");
        resMI.registerTempTable("MITable3");
        //==============================================================================================================
        //calculate condEntropyTweetTrueContainTermFalse
        results2 = sqlContext.sql("select term, (" + containNotContainCounts[0] + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueContainTermTrue");
        fromresults2 = results2.join(containTermProb, containTermProb.col("term1").equalTo(results2.col("term"))).drop("term1");
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - (row.getDouble(2)/tweetNum)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable4");
        resMI = sqlContext.sql("SELECT mt1.term, (mt1.prob+mt2.prob) AS prob FROM MITable3 mt1, MITable4 mt2 where mt1.term = mt2.term");
        resMI.registerTempTable("MITable5");
        //==============================================================================================================
        //calculate condEntropyTweetFalseContainTermFalse
        results2 = sqlContext.sql("select term, (" + containNotContainCounts[1] + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseContainTermTrue");
        fromresults2 = results2.join(containTermProb, containTermProb.col("term1").equalTo(results2.col("term"))).drop("term1");
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - (row.getDouble(2)/tweetNum)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable6");
        StructField[] resFields = {
                DataTypes.createStructField("feature", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("mutualEntropy", DataTypes.DoubleType, true)
        };
        resMI = sqlContext.createDataFrame(sqlContext.sql("SELECT mt1.term, (mt1.prob+mt2.prob) AS mutualEntropy FROM MITable5 mt1, MITable6 mt2 where mt1.term = mt2.term").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                return RowFactory.create("term", v1.getString(0), v1.getDouble(1));
            }
        }), new StructType(resFields));
        resMI = resMI.sort(resMI.col("mutualEntropy").desc());//.limit(topFeatureNum).coalesce(numPart);
        output(resMI, "TopMIfeatures/" + groupNames[groupNum - 1] + "/top_Term", false);
    }
}
