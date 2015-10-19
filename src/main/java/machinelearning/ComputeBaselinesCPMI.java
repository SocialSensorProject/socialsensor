package machinelearning;

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
import java.util.Collections;
import java.util.List;

/**
 * Created by zahraiman on 8/10/15.
 */
public class ComputeBaselinesCPMI {
    private static String hdfsPath;
    private static int numPart;
    private static double tweetCount;
    private static SQLContext sqlContext;
    private static DataFrame tweet_user_hashtag_grouped;
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
    private static final boolean calcToUser = false;
    private static final boolean calcContainHashtag = false;
    private static final boolean calcContainTerm = false;
    private static final boolean calcContainLocation = false;
    private static final boolean writeTopicalLocation = false;
    private static final boolean writeTopicalTerm = false;
    private static final boolean writeTopicalFrom = true;
    private static final boolean writeTopicalHashtag = false;
    private static final boolean writeTopicalMention = false;
    private static int groupNum;
    private static int numOfGroups;
    private static String[] groupNames;
    private static TweetUtil tweetUtil = new TweetUtil();
    private static DataFrame tweetTime;
    private static final int topFeatureNum = 1000;

    private static final long[] timestamps= {1377897403000l, 1362146018000l, 1391295058000l, 1372004539000l, 1359920993000l, 1364938764000l, 1378911100000l, 1360622109000l, 1372080004000l, 1360106035000l};;


    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String[] args) throws IOException, ParseException {
        loadConfig();
        //long t1 = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse("Fri Feb 28 11:00:00 +0000 2014").getTime();
        numOfGroups = configRead.getNumOfGroups();
        groupNames = configRead.getGroupNames();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        dataPath = hdfsPath + configRead.getDataPath();
        outputPath = hdfsPath + configRead.getOutputPath();
        localRun = configRead.isLocal();
        topUserNum = configRead.getTopUserNum();

        for(groupNum = 6; groupNum <= 6; groupNum++) {
            if(groupNum > 1 && groupNum != 6)
                continue;
            initializeSqlContext();
            System.out.println("============================"+groupNum+"=============================");
            if(calcToUser)
                calcToUserProb(tweetCount);

            if(calcContainHashtag)
                calcContainHashtagProb(tweetCount);

            if(calcFromUser)
                calcFromUserProb(tweetCount);

            if(calcContainTerm)
                calcContainTermProb(tweetCount);

            if(calcContainLocation)
                calcContainLocationProb(tweetCount);

            containNotContainCounts = getContainNotContainCounts(groupNum);

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
        System.out.println("======================GROUPNUM: " + groupNum + dataPath + "tweet_time_parquet");
        tweetTime = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);
        StructField[] timeField = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        final int grNum = groupNum;
        tweetTime = sqlContext.createDataFrame(tweetTime.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getLong(1) <= timestamps[grNum-1];
            }
        }), new StructType(timeField)).coalesce(numPart);
        tweetTime.cache();
        tweetCount = tweetTime.count();
        System.out.println("=================================Tweet Time Size: " + tweetCount);
        tweet_user_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);
        tweet_user_hashtag_grouped = tweet_user_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_user_hashtag_grouped.col("tid")), "inner").drop(tweetTime.col("tid")).coalesce(numPart);
        //tweet_user_hashtag_grouped.drop("time").write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_user_hashtag_grouped_parquet" + "_time");
        //tweet_user_hashtag_grouped.cache();
        //System.out.println("LOOOOOOOOK-FromUSER: " + tweet_user_hashtag_grouped.count());
        System.out.println(" HAS READ THE TWEET_HASHTAG ");
        //TweetUtil tutil = new TweetUtil();
        //tutil.runStringCommand("mkdir "+groupNames[groupNum-1]+"/");
    }

    public static DataFrame calcFromToProb(final double tweetNum, DataFrame df, String colName, String probName, String tableName){
        df = df.join(tweetTime, tweetTime.col("tid").equalTo(df.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
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


    public static void calcContainHashtagProb(final double tweetNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        fromHashtagProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet").coalesce(numPart), "hashtag1", "fromProb", "tweet_hashtag_hashtag_grouped_parquet");
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
        fromUserProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart), "username1", "fromProb", "tweet_user_hashtag_grouped_parquet");
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

    public static void calcContainTermProb(final double tweetNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        containTermProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").coalesce(numPart), "term1", "containTermProb", "tweet_term_hashtag_grouped_parquet");
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

    public static void calcToUserProb(final double tweetNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        toUserProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").coalesce(numPart), "username1", "toProb", "tweet_mention_hashtag_grouped_parquet");
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

    public static void calcContainLocationProb(final double tweetNum){
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        containLocationProb = calcFromToProb(tweetCount, sqlContext.read().parquet(dataPath + "tweet_location_hashtag_grouped_parquet").coalesce(numPart), "username1", "locProb", "tweet_location_hashtag_grouped_parquet");
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
        final double probTweetContain = (double) containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double) containNotContainCounts[1] / tweetCount;
        DataFrame tweet_mention_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").coalesce(numPart);
        tweet_mention_hashtag_grouped = tweet_mention_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_mention_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweet_mention_hashtag_grouped.cache();
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

        //output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueToUserTrue_" + groupNum, false);
        results2=sqlContext.createDataFrame(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / (row.getDouble(2)/tweetNum)));
            }
        }), new StructType(fields1)).coalesce(numPart);
        results2 = results2.sort(results2.col("prob").desc()).limit(topFeatureNum).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CP/top1000_Mention", false);
        results2=sqlContext.createDataFrame(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), (Math.log(row.getDouble(1))));
            }
        }), new StructType(fields1)).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CPLog/top1000_Mention", false);
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1-(row.getDouble(2)/tweetNum)))));
            }
        })).coalesce(numPart);
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

        toresults2 = sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetToUser GROUP BY username");
        toresults2 = toresults2.sort(toresults2.col("mutualEntropy").desc()).limit(topFeatureNum).coalesce(numPart);
        output(toresults2, "Baselines/" + groupNames[groupNum-1] + "/MI/top1000_Mention", false);
        toresults2=sqlContext.createDataFrame(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), Math.log(Double.valueOf(row.get(1).toString())));
            }
        }), new StructType(fields1)).coalesce(numPart);
        output(toresults2, "Baselines/" + groupNames[groupNum-1] + "/MILog/top1000_Mention", false);
    }


    public static void calcTweetCondContainLocationConditionalEntropy(final int groupNum) throws IOException {
        final double tweetNum = tweetCount;
        StructField[] fields1 = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        final double probTweetContain = (double) containNotContainCounts[0] / tweetCount;
        final double probTweetNotContain = (double) containNotContainCounts[1] / tweetCount;
        DataFrame tweet_location_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_location_hashtag_grouped_parquet").coalesce(numPart);
        tweet_location_hashtag_grouped = tweet_location_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_location_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
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
        //output(results2.sort(results2.col("prob").desc()).coalesce(1), "ProbTweetTrueToUserTrue_" + groupNum, false);
        results2=sqlContext.createDataFrame(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / (row.getDouble(2) / tweetNum)));
            }
        }), new StructType(fields1));
        results2 = results2.sort(results2.col("prob").desc()).limit(topFeatureNum).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CP/top1000_Location", false);
        results2=sqlContext.createDataFrame(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), (Math.log(row.getDouble(1))));
            }
        }), new StructType(fields1)).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CPLog/top1000_Location", false);
        //System.out.println("SIZE 1 TO =================" + toresMI.count() + "================");
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
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1 - (row.getDouble(2)/tweetNum)))));
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

        toresults2 = sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetContainLocation GROUP BY username").coalesce(numPart);
        toresults2 = toresults2.sort(toresults2.col("mutualEntropy").desc()).limit(topFeatureNum).coalesce(numPart);
        output(toresults2, "Baselines/" + groupNames[groupNum-1] + "/MI/top1000_Location", false);
        toresults2=sqlContext.createDataFrame(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), Math.log(Double.valueOf(row.get(1).toString())));
            }
        }), new StructType(fields1)).coalesce(numPart);
        output(toresults2, "Baselines/" + groupNames[groupNum-1] + "/MILog/top1000_Location", false);

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
        //TODO should I cache some of the user probabilities in the memory
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
        results2 = sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / (row.getDouble(2)/tweetNum)));
            }
        }), new StructType(fields));
        results2 = results2.sort(results2.col("prob").desc()).limit(topFeatureNum).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CP/top1000_From", false);
        results2=sqlContext.createDataFrame(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), (Math.log(row.getDouble(1))));
            }
        }), new StructType(fields)).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum - 1] + "/CPLog/top1000_From", false);
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
        resMI = sqlContext.sql("SELECT mt1.username as username, (mt1.prob+mt2.prob) AS mutualEntropy FROM MITable5 mt1, MITable6 mt2 where mt1.username = mt2.username").coalesce(numPart);
        resMI = resMI.sort(resMI.col("mutualEntropy").desc()).limit(topFeatureNum).coalesce(numPart);
        output(resMI, "Baselines/" + groupNames[groupNum-1] + "/MI/top1000_From", false);
        resMI=sqlContext.createDataFrame(resMI.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), Math.log(Double.valueOf(row.get(1).toString())));
            }
        }), new StructType(fields)).coalesce(numPart);
        output(resMI, "Baselines/" + groupNames[groupNum-1] + "/MILog/top1000_From", false);;


        /*DataFrame countDf;
        String[] countNames = {"CSVOut_user_favoriteCount_parquet.csv", "CSVOut_user_friendsCount_parquet.csv", "CSVOut_user_followerCount_parquet.csv"};
        for(String countName: countNames) {
            countDf = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "user_followersCount_paruqet.csv").coalesce(numPart);
            countDf.printSchema();
            countDf = sqlContext.createDataFrame(countDf.javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return Long.valueOf(v1.get(1).toString()) > 50;
                }
            }), new StructType(countFields)).coalesce(numPart);
            resMI = resMI.join(countDf, resMI.col("username").equalTo(countDf.col("username1"))).drop(countDf.col("username1")).coalesce(numPart);
            resMI.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + "CSVOut_user_"+countName+"_paruqet.csv");
        }*/
    }


    private static void output(DataFrame data, String folderName, boolean flag) {
        if(flag)
            data.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/" + folderName + "_csv");
        //data.write().mode(SaveMode.Overwrite).parquet(outputPath+"TestTrain/"+ groupNames[groupNum-1] +"/"+ folderName + "_parquet");
        data.coalesce(numPart).write().mode(SaveMode.Overwrite).parquet(outputPath+folderName + "_parquet");
    }

    private static JavaRDD<Row> calcProb(DataFrame df, final int groupNum, final boolean containFlag, final double tweetNum) throws IOException {
        final List<String> hashtagList = tweetUtil.getTestTrainGroupHashtagList(groupNum, localRun, true);
        return df.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                int numHashtags= 0;
                if(row.get(2) != null) {
                    List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(2).split(","))));
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
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2() / tweetNum);
            }
        });
    }

    public static long[] getContainNotContainCounts(final int groupNum){
        long[] counts = new long[2];
        if(groupNum == 1) {
            counts[0] = 9425;
            counts[1] = 286355255;
        }else if(groupNum == 6){
            counts[0] = 9690;
            counts[1] = 84100572;
        }else {
            JavaRDD<Row> containNotContainNum = tweet_user_hashtag_grouped.drop("username").coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Integer, Long>() {
                @Override
                public Tuple2<Integer, Long> call(Row row) throws Exception {
                    List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(1).split(","))));
                    tH.retainAll(tweetUtil.getTestTrainGroupHashtagList(groupNum, localRun, true));
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
        }
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
        //TODO should I cache some of the user probabilities in the memory
        DataFrame tweet_hashtag_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet").coalesce(numPart);
        tweet_hashtag_hashtag_grouped = tweet_hashtag_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_hashtag_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
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
        results2=sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / (row.getDouble(2)/tweetNum)));
            }
        }), new StructType(fields));
        results2 = results2.sort(results2.col("prob").desc()).limit(topFeatureNum).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CP/top1000_Hashtag", false);
        results2=sqlContext.createDataFrame(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), (Math.log(row.getDouble(1))));
            }
        }), new StructType(fields)).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CPLog/top1000_Hashtag", false);
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
        fromresults2 = sqlContext.sql("SELECT hashtag, sum(prob) AS mutualEntropy FROM mutualEntropyTweetContainHashtag GROUP BY hashtag").coalesce(numPart);
        fromresults2 = fromresults2.sort(fromresults2.col("mutualEntropy").desc()).limit(topFeatureNum).coalesce(numPart);
        output(fromresults2, "Baselines/" + groupNames[groupNum-1] + "/MI/top1000_Hashtag", false);
        fromresults2=sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), Math.log(Double.valueOf(row.get(1).toString())));
            }
        }), new StructType(fields)).coalesce(numPart);
        output(fromresults2, "Baselines/" + groupNames[groupNum-1] + "/MILog/top1000_Hashtag", false);;


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
        DataFrame tweet_term_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").coalesce(numPart);
        tweet_term_hashtag_grouped = tweet_term_hashtag_grouped.join(tweetTime, tweetTime.col("tid").equalTo(tweet_term_hashtag_grouped.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
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
        results2=sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), (row.getDouble(1) / (row.getDouble(2) / tweetNum)));
            }
        }), new StructType(fields));
        results2 = results2.sort(results2.col("prob").desc()).limit(topFeatureNum).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CP/top1000_Term", false);
        results2=sqlContext.createDataFrame(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), (Math.log(row.getDouble(1))));
            }
        }), new StructType(fields)).coalesce(numPart);
        output(results2, "Baselines/" + groupNames[groupNum-1] + "/CPLog/top1000_Term", false);
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
        resMI = sqlContext.sql("SELECT mt1.term, (mt1.prob+mt2.prob) AS mutualEntropy FROM MITable5 mt1, MITable6 mt2 where mt1.term = mt2.term").coalesce(numPart);
        resMI = resMI.sort(resMI.col("mutualEntropy").desc()).limit(topFeatureNum).coalesce(numPart);
        output(fromresults2, "Baselines/" + groupNames[groupNum - 1] + "/MI/top1000_Term", false);
        resMI.cache();
        resMI=sqlContext.createDataFrame(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                return RowFactory.create(row.getString(0), Math.log(Double.valueOf(row.get(1).toString())));
            }
        }), new StructType(fields)).coalesce(numPart);
        output(fromresults2, "Baselines/" + groupNames[groupNum - 1] + "/MILog/top1000_Term", false);
    }

}
