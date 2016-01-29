package machinelearning.MutualInformation;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by zahraiman on 8/10/15.
 */
public class ComputeTopicsCPMI {
    private static String hdfsPath;
    private static int numPart;
    private static double tweetCount;
    private static SQLContext sqlContext;
    private static DataFrame tweet_user_hashtag_grouped;
    private static DataFrame fromUserProb;
    private static DataFrame containTermProb;
    private static DataFrame toUserProb;
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
    private static int groupNum;
    private static int numOfGroups;
    private static String[] groupNames;
    private static TweetUtil tweetUtil = new TweetUtil();


    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String[] args) throws IOException {
        loadConfig();
        numOfGroups = configRead.getNumOfGroups();
        groupNames = configRead.getGroupNames();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        dataPath = hdfsPath + configRead.getDataPath();
        outputPath = hdfsPath + configRead.getOutputPath();
        localRun = configRead.isLocal();
        topUserNum = configRead.getTopUserNum();

        initializeSqlContext();
        if(calcToUser)
            calcToUserProb(tweetCount);

        if(calcContainHashtag)
            calcContainHashtagProb(tweetCount);

        if(calcFromUser)
            calcFromUserProb(tweetCount);

        if(calcContainTerm)
            calcContainTermProb(tweetCount);

        for(groupNum = 2; groupNum <= numOfGroups; groupNum++) {
            System.out.println("============================"+groupNum+"=============================");
            containNotContainCounts = getContainNotContainCounts(groupNum);
            if (calcToUser)
                calcTweetCondToUserConditionalEntropy(groupNum);

            if (calcContainHashtag)
                calcTweetCondContainHashtagConditionalEntropy(groupNum);

            if (calcFromUser)
                calcTweetCondFromUserConditionalEntropy(groupNum);

            if (calcContainTerm)
                calcTweetCondContainTermConditionalEntropy(groupNum);
        }
    }

    public static void initializeSqlContext(){
        SparkConf sparkConfig;
        if(localRun) {
            dataPath = configRead.getTestDataPath();
            //dataPath = "TestSet/data1Month/";
            outputPath = configRead.getTestOutPath();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[2]").set("spark.executor.memory", "6g").set("spark.driver.maxResultSize", "6g");
            tweetCount = 100;
        }else {
            tweetCount = 829026458; //tweet_user.count();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics");
        }
        sparkContext = new JavaSparkContext(sparkConfig);
        sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);
        System.out.println("LOOK: " + dataPath + "tweet_user_hashtag_grouped_parquet");
        tweet_user_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);
        tweet_user_hashtag_grouped.cache();

        System.out.println("LOOOOOOOOK-FromUSER: " + tweet_user_hashtag_grouped.count());
        System.out.println(" HAS READ THE TWEET_HASHTAG ");
        //TweetUtil tutil = new TweetUtil();
        //tutil.runStringCommand("mkdir "+groupNames[groupNum]+"/");
    }

    public static DataFrame calcFromToProb(final double tweetNum, DataFrame df, String colName, String probName){
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
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2() / tweetNum);
            }
        });
        StructField[] fields = {
                DataTypes.createStructField(colName, DataTypes.StringType, true),
                DataTypes.createStructField(probName, DataTypes.DoubleType, true),
        };
        return sqlContext.createDataFrame(prob, new StructType(fields));
    }


    public static void calcContainHashtagProb(final double tweetNum){
        fromHashtagProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart), "hashtag1", "fromProb");
        fromHashtagProb.registerTempTable("fromHashtagProb");
        fromHashtagProb.cache();
    }

    public static void calcFromUserProb(final double tweetNum){
        fromUserProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart), "username1", "fromProb");
        fromUserProb.registerTempTable("fromUserProb");
        fromUserProb.cache();
    }

    public static void calcContainTermProb(final double tweetNum){
        containTermProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart), "term1", "containTermProb");
        containTermProb.registerTempTable("containTermProb");
        containTermProb.cache();
    }

    public static void calcToUserProb(final double tweetNum){
        toUserProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_mention_parquet").coalesce(numPart), "username1", "toProb");
        toUserProb.registerTempTable("toUserProb");
        toUserProb.cache();

    }

    /*
      * groupNum: Hashtag Topic Group Number
      * (tweet_Contain_topical_Hashtag | Mention_user)
    */
    public static void calcTweetCondToUserConditionalEntropy(final int groupNum) {
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        //TODO should I cache some of the user probabilities in the memory
        final double probTweetContain = (double) containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double) containNotContainCounts[1] / tweetCount;
        DataFrame tweet_mention_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").coalesce(numPart);
        tweet_mention_hashtag_grouped.cache();
        System.out.println("LOOOOOOOOK-ToUSER: " + tweet_mention_hashtag_grouped.count());
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * row.getDouble(2))));
            }
        }).coalesce(numPart);
        toresMI.count();
        //output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueToUserTrue_" + groupNum, false);
        results2=sqlContext.createDataFrame(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / row.getDouble(2)));
            }
        }), new StructType(fields1));
        output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueCondToUserTrue_" + groupNum, false);
        //System.out.println("SIZE 1 TO =================" + toresMI.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet = calcProb(tweet_mention_hashtag_grouped, groupNum, false, tweetCount);
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
            }
        })).coalesce(numPart);
        System.out.println("SIZE 2 to=================" + toresMI.count() + "================");
        //==============================================================================================================
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[0] + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueToUserTrue");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1-row.getDouble(2)))));
            }
        })).coalesce(numPart);
        System.out.println("SIZE 3 to =================" + toresMI.count() + "================");
        //==============================================================================================================
        results2 = sqlContext.sql("select username, (" + containNotContainCounts[1] + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseToUserTrue");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1-row.getDouble(2)))));
            }
        })).coalesce(numPart);
        System.out.println("SIZE 4 to=================" + toresMI.count() + "================");
        sqlContext.createDataFrame(toresMI, new StructType(fields1)).coalesce(numPart).registerTempTable("mutualEntropyTweetToUser");

        toresults2 = sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetToUser GROUP BY username");
        output(toresults2.sort(toresults2.col("mutualEntropy").desc()).coalesce(1), "mutualEntropyTweetToUser_"+groupNum, false);

    }

    /*
      * groupNum: Hashtag Topic Group Number
      * (tweet_Contain_topical_Hashtag | FromUser)
      *
    */
    public static void calcTweetCondFromUserConditionalEntropy(final int groupNum){
        StructField[] fields = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
        //TODO should I cache some of the user probabilities in the memory
        //==============================================================================================================
        final double probTweetContain = (double)containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double)containNotContainCounts[1] / tweetCount;

        JavaRDD<Row> probContainTweet = calcProb(tweet_user_hashtag_grouped, groupNum, true, tweetCount);
        System.out.println("LOOOOOOOOK-FromUSER: " + tweet_user_hashtag_grouped.count());
        DataFrame results2 = sqlContext.createDataFrame(probContainTweet.coalesce(numPart), new StructType(fields));
        results2.registerTempTable("condEntropyTweetTrueFromUserTrue");
        //CE_P(Y = true|  FromUser = true)
        DataFrame fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = true|  FromUser = true)
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (probTweetContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * row.getDouble(2))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable1");
        results2 = sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / row.getDouble(2)));
            }
        }), new StructType(fields));
        output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueCondFromUserTrue_" + groupNum, false);
        //==============================================================================================================
        JavaRDD<Row> probNotContainTweet = calcProb(tweet_user_hashtag_grouped, groupNum, false, tweetCount);
        results2 = (sqlContext.createDataFrame(probNotContainTweet.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        //MI_P(Y = False|  FromUser = true)
        sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (probTweetNotContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable2");
        DataFrame resMI = sqlContext.sql("SELECT mt1.username, (mt1.prob+mt2.prob) AS prob FROM MITable1 mt1, MITable2 mt2 where mt1.username = mt2.username");
        System.out.println("SIZE 2=================" + resMI.count() + "================");
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - row.getDouble(2)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable4");
        resMI = sqlContext.sql("SELECT mt1.username, (mt1.prob+mt2.prob) AS prob FROM MITable3 mt1, MITable4 mt2 where mt1.username = mt2.username");
        System.out.println("SIZE 3=================" + resMI.count() + "================");
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - row.getDouble(2)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable6");
        resMI = sqlContext.sql("SELECT mt1.username, (mt1.prob+mt2.prob) AS mutualEntropy FROM MITable5 mt1, MITable6 mt2 where mt1.username = mt2.username");
        resMI.cache();
        System.out.println("SIZE 4=================" + resMI.count() + "================");
        resMI.registerTempTable("mutualEntropyTweetFromUser");
        output(resMI.sort(resMI.col("mutualEntropy").desc()).coalesce(1), "mutualEntropyTweetFromUser_"+groupNum, false);
    }


    private static void output(DataFrame data, String folderName, boolean flag) {
        if(flag)
            data.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/" + folderName + "_csv");
        data.write().mode(SaveMode.Overwrite).parquet(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/"+ folderName + "_parquet");
    }

    private static JavaRDD<Row> calcProb(DataFrame df, final int groupNum, final boolean containFlag, final double tweetNum){
        final List<String> hashtagList = tweetUtil.getGroupHashtagList(groupNum, localRun);
        return df.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(2).split(","))));
                tH.retainAll(hashtagList);
                int numHashtags = tH.size();
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
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2() / tweetNum);
            }
        });
    }

    public static long[] getContainNotContainCounts(final int groupNum){
        JavaRDD<Row> containNotContainNum =  tweet_user_hashtag_grouped.drop("username").coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(Row row) throws Exception {
                List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(1).split(","))));
                tH.retainAll(tweetUtil.getGroupHashtagList(groupNum, localRun));
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
        long [] counts = new long[2];
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

    public static void calcTweetCondContainHashtagConditionalEntropy(final int groupNum){
        StructField[] fields = {
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
        //TODO should I cache some of the user probabilities in the memory
        DataFrame tweet_hashtag_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet").coalesce(numPart);
        tweet_hashtag_hashtag_grouped.cache();
        System.out.println("LOOOOOOOOK-Hashtag: " + tweet_hashtag_hashtag_grouped.count());
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * row.getDouble(2))));
            }
        });
        results2=sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / row.getDouble(2)));
            }
        }), new StructType(fields));
        output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueCondContainHashtagTrue_" + groupNum, false);
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - row.getDouble(2)))));
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 4 Hashtag=================" + resMI.count() + "================");
        sqlContext.createDataFrame(resMI.coalesce(numPart), new StructType(fields)).registerTempTable("mutualEntropyTweetContainHashtag");

        //======================== COMPUTE COND ENTROPY=================================================================
        fromresults2 = sqlContext.sql("SELECT hashtag, sum(prob) AS mutualEntropy FROM mutualEntropyTweetContainHashtag GROUP BY hashtag");
        output(fromresults2.sort(fromresults2.col("mutualEntropy").desc()).coalesce(1), "mutualEntropyTweetContainHashtag_"+groupNum, false);

    }

    /*
    *
    * (Tweet_contain_topical_hashtag | contain_term)
    *
     */

    public static void calcTweetCondContainTermConditionalEntropy(final int groupNum){
        StructField[] fields = {
                DataTypes.createStructField("term", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
        DataFrame tweet_term_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").coalesce(numPart);
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * row.getDouble(2))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable1");
        results2=sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / row.getDouble(2)));
            }
        }), new StructType(fields));
        output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueCondContainTermTrue_" + groupNum, false);
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - row.getDouble(2)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable4");
        resMI = sqlContext.sql("SELECT mt1.term, (mt1.prob+mt2.prob) AS prob FROM MITable3 mt1, MITable4 mt2 where mt1.term = mt2.term");
        System.out.println("SIZE 3 Term=================" + resMI.count() + "================");
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1 - row.getDouble(2)))));
            }
        }).coalesce(numPart), new StructType(fields)).registerTempTable("MITable6");
        resMI = sqlContext.sql("SELECT mt1.term, (mt1.prob+mt2.prob) AS mutualEntropy FROM MITable5 mt1, MITable6 mt2 where mt1.term = mt2.term");
        resMI.cache();
        System.out.println("SIZE 4 Term=================" + resMI.count() + "================");
        resMI.registerTempTable("mutualEntropyTweetContainTerm");
        output(resMI.sort(resMI.col("mutualEntropy").desc()).coalesce(1), "mutualEntropyTweetContainTerm_"+groupNum, false);

    }

}
