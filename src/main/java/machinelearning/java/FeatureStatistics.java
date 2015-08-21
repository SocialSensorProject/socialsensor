package machinelearning.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import preprocess.spark.ConfigRead;
import scala.Tuple2;
import scala.collection.Seq;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by zahraiman on 8/10/15.
 */
public class FeatureStatistics {
    private static String hdfsPath;
    private static int numPart;
    private static DataFrame tweet_user;
    private static double tweetCount;
    private static SQLContext sqlContext;
    private static DataFrame tweet_user_hashtag_grouped;
    private static DataFrame fromUserProb;
    private static DataFrame toUserProb;
    private static DataFrame fromHashtagProb;
    private static boolean testSet = false;
    private static int topUserNum;
    private static boolean localRun;
    private static ConfigRead configRead;
    private static DataFrame tweet_hashtag;
    private static JavaSparkContext sparkContext;
    private static DataFrame tweet_hashtag_hashtag_grouped;


    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    private static String dataPath;
    private static String outputPath; //"Local_Results/out/";

    public static void main(String[] args) throws IOException {
        loadConfig();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        dataPath = hdfsPath + configRead.getDataPath();
        outputPath = hdfsPath + configRead.getOutputPath();
        localRun = configRead.isLocal();
        topUserNum = configRead.getTopUserNum();
        int groupNum = 3;
        initializeSqlContext();
        calcFromUserProb(tweetCount);
        calcToUserProb(tweetCount);
        calcTweetCondFromUserConditionalEntropy(groupNum);

        calcFromHashtagProb(tweetCount);
        calcTweetCondFromHashtagConditionalEntropy(groupNum);
    }

    public static void initializeSqlContext(){
        SparkConf sparkConfig;
        if(localRun) {
            dataPath = configRead.getTestDataPath();
            outputPath = configRead.getTestOutPath();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[2]").set("spark.executor.memory", "6g").set("spark.driver.maxResultSize", "6g");
        }else {
            tweetCount = 829026458; //tweet_user.count();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics");
        }
        sparkContext = new JavaSparkContext(sparkConfig);
        sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);
        tweet_hashtag = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart);
        tweet_user = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);
        tweet_user.cache();
        tweet_user_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);//.registerTempTable("tweet_user_hashtag_grouped");
        tweet_user_hashtag_grouped.cache();
        tweet_hashtag_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet").coalesce(numPart);
        tweet_hashtag_hashtag_grouped.cache();
        System.out.println(" HAS READ THE TWEET_HASHTAG ");
        if(localRun)
            tweetCount = tweet_user.count();//TODO change hardcode numbers
    }

    public static void calcFromHashtagProb(final double tweetNum){
        JavaRDD<Row> prob = tweet_hashtag.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
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
                if(tweetNum == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2() / tweetNum);
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("hashtag1", DataTypes.StringType, true),
                DataTypes.createStructField("fromProb", DataTypes.DoubleType, true),
        };
        //userFromProbMap = userFromProb.collectAsMap();
        fromHashtagProb = sqlContext.createDataFrame(prob, new StructType(fields));
        fromHashtagProb.registerTempTable("fromHashtagProb");
        fromHashtagProb.cache();
    }

    public static void calcFromUserProb(final double tweetNum){
        JavaRDD<Row> userFromProb = tweet_user.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
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
                if(tweetNum == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2() / tweetNum);
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("username1", DataTypes.StringType, true),
                DataTypes.createStructField("fromProb", DataTypes.DoubleType, true),
        };
        //userFromProbMap = userFromProb.collectAsMap();
        fromUserProb = sqlContext.createDataFrame(userFromProb, new StructType(fields));
        fromUserProb.registerTempTable("fromUserProb");
        fromUserProb.cache();
    }

    public static void calcToUserProb(final double tweetNum){
        DataFrame tweet_mention = sqlContext.read().parquet(dataPath + "tweet_mention_parquet").coalesce(numPart);
        JavaRDD<Row> userToProb = tweet_mention.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
            @Override
            public Tuple2<String, Double> call(Row row) throws Exception {
                //TODO This is true when a user is only mentioned once in a tweet
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
                DataTypes.createStructField("username1", DataTypes.StringType, true),
                DataTypes.createStructField("toProb", DataTypes.DoubleType, true),
        };
        toUserProb = sqlContext.createDataFrame(userToProb, new StructType(fields));
        toUserProb.cache();
        toUserProb.registerTempTable("toUserProb");
    }


    public static void calcTweetCondFromUserConditionalEntropy(final int groupNum){
        //System.out.println("==================User count: " + tweet_user.drop("tid").distinct().count()); // LOCAL: 11534925
        //System.out.println("==================User count From hashtag: " + tweet_user_hashtag_grouped.drop("tid").drop("hashtag").distinct().count()); // LOCAL: 11534925
        //System.out.println("==================Tweet count From hashtag: " + tweet_user_hashtag_grouped.drop("username").drop("hashtag").distinct().count()); // LOCAL: 11534925
        //int UniqueUserCount = 95547198;
        StructField[] fields = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
        //TODO should I cache some of the user probabilities in the memory
        //==============================================================================================================
        JavaRDD<Row> containNotContainNum = calcProb(groupNum, true, 3, tweetCount);
        StructField[] fields1 = {
                DataTypes.createStructField("key", DataTypes.IntegerType, true),
                DataTypes.createStructField("num", DataTypes.LongType, true),
        };
        (sqlContext.createDataFrame(containNotContainNum, new StructType(fields1))).registerTempTable("containNumTable");
        long containNum = sqlContext.sql("select num from containNumTable where key = 1").head().getLong(0);
        long notContainNum = sqlContext.sql("select num from containNumTable where key = 2").head().getLong(0);
        final double probTweetContain = (double)containNum / tweetCount;
        final double probTweetNotContain = (double)notContainNum / tweetCount;

        JavaRDD<Row> condEntropyTweetTrueFromUserTrue = calcProb(groupNum, true, 1, tweetCount);
        DataFrame results2 = sqlContext.createDataFrame(condEntropyTweetTrueFromUserTrue.coalesce(numPart), new StructType(fields));
        results2.registerTempTable("condEntropyTweetTrueFromUserTrue");

        DataFrame fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        JavaRDD<Row> res = fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        });
        JavaRDD<Row> resMI = fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(probTweetContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * row.getDouble(2))));
            }
        });
        System.out.println("SIZE 1=================" + resMI.count() + "================" );

        DataFrame toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        JavaRDD<Row> tores = toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        });
        JavaRDD<Row> toresMI = toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1)/(probTweetContain*row.getDouble(2))));
            }
        });
        System.out.println("SIZE 1 TO ================="+ toresMI.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> condEntropyTweetFalseFromUserTrue = calcProb(groupNum, false, 2, tweetCount);
        results2 = (sqlContext.createDataFrame(condEntropyTweetFalseFromUserTrue.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (probTweetNotContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
            }
        }));
        System.out.println("SIZE 2=================" + resMI.count() + "================");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        tores = tores.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
            }
        }));
        double fromSize = (double) resMI.count(); double toSize = (double)toresMI.count();
        System.out.println("SIZE 2 to=================" + toresMI.count() + "================");
        //==============================================================================================================


        results2 = sqlContext.sql("select username, (" + containNum + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (1-row.getDouble(2)))); // 1- fromUserProb = NotFromUserProb
            }
        }));
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 3=================" + resMI.count() + "================");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        tores = tores.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (1-row.getDouble(2))));
            }
        }));
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 3 to =================" + toresMI.count() + "================");
        //==============================================================================================================

        results2 = sqlContext.sql("select username, (" + notContainNum + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (1-row.getDouble(2))));
            }
        }));
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (probTweetNotContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 4=================" + resMI.count() + "================");
        sqlContext.createDataFrame(res.coalesce(numPart), new StructType(fields)).registerTempTable("condEntropyTweetFromUser");
        sqlContext.createDataFrame(resMI.coalesce(numPart), new StructType(fields)).registerTempTable("mutualEntropyTweetFromUser");

        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        tores = tores.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (1-row.getDouble(2))));
            }
        }));
        toresMI = toresMI.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 4 to=================" + toresMI.count() + "================");
        sqlContext.createDataFrame(tores.coalesce(numPart), new StructType(fields)).registerTempTable("condEntropyTweetToUser");
        sqlContext.createDataFrame(toresMI.coalesce(numPart), new StructType(fields)).registerTempTable("mutualEntropyTweetToUser");

        //======================== COMPUTE COND ENTROPY=================================================================
        fromresults2 = sqlContext.sql("SELECT username, -sum(prob) AS condEntropy FROM condEntropyTweetFromUser GROUP BY username");
        fromresults2 = fromresults2.sort(fromresults2.col("condEntropy"));//.limit((int) fromSize / 2 + topUserNum / 2);
        fromresults2.cache();
        //output(sqlContext.createDataFrame(takeMiddle(fromresults2, sparkContext), new StructType(fields)), "CondEntropyTweetFromUser_"+groupNum+"_middle", false);
        //output(fromresults2.limit(topUserNum), "CondEntropyTweetFromUser_"+groupNum+"_top", false);
        System.out.println("GROUP NUM: " + groupNum);
        output(fromresults2.coalesce(1), "CondEntropyTweetFromUser", false);



        toresults2 = sqlContext.sql("SELECT username, -sum(prob) AS condEntropy FROM condEntropyTweetToUser GROUP BY username");
        toresults2 = toresults2.sort(toresults2.col("condEntropy"));//.limit((int) toSize / 2 + topUserNum / 2);
        //output(sqlContext.createDataFrame(takeMiddle(toresults2, sparkContext), new StructType(fields)), "CondEntropyTweetToUser_"+groupNum+"_middle", false);
        //output(toresults2.limit(topUserNum), "CondEntropyTweetToUser_"+groupNum+"_top", false);
        output(toresults2.coalesce(1), "CondEntropyTweetToUser", false);

        fromresults2 = sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetFromUser GROUP BY username");
        fromresults2 = fromresults2.sort(fromresults2.col("mutualEntropy"));
        //output(sqlContext.createDataFrame(takeMiddle(fromresults2, sparkContext), new StructType(fields)), "mutualEntropyTweetFromUser_"+groupNum+"_middle", false);
        //output(fromresults2.limit(topUserNum), "mutualEntropyTweetFromUser_"+groupNum+"_top", false);
        output(fromresults2.coalesce(1), "mutualEntropyTweetFromUser", false);

        toresults2 = sqlContext.sql("SELECT username, sum(prob) AS mutualEntropy FROM mutualEntropyTweetToUser GROUP BY username");
        toresults2 = toresults2.sort(toresults2.col("mutualEntropy"));
        //output(sqlContext.createDataFrame(takeMiddle(toresults2, sparkContext), new StructType(fields)), "mutualEntropyTweetToUser_"+groupNum+"_middle", false);
        //output(toresults2.limit(topUserNum), "mutualEntropyTweetToUser_" + groupNum + "_top", false);
        output(toresults2.coalesce(1), "mutualEntropyTweetToUser", false);



        //==============================================================================================================
        /*sqlContext.sql("select sum(t1.prob, t2.prob, t3.prob, t4.prob) from condEntropyTweetTrueFromUserTrue t1  INNER JOIN " +
                "condEntropyTweetFalseFromUserTrue t2 on t1.username = t2.username  INNER JOIN" +
                "condEntropyTweetTrueFromUserFalse t3 on t2.username = t3.username  INNER JOIN" +
                "condEntropyTweetFalseFromUserFalse t4 on t3.username = t4.username");*/
    }

    private static JavaRDD<Row> takeMiddle(DataFrame res, JavaSparkContext sparkContext) { // res is already sorted
        double size = (double)res.count();
        for(Row r: res.collectAsList().subList((int)Math.floor((size / 2) - (topUserNum / 2)),(int) Math.ceil(size / 2) + (topUserNum / 2)))
            System.out.println(r.getString(0) + ", " + r.getDouble(1));
        return null;
        //return sparkContext.parallelize(res.collectAsList().subList((int)Math.floor((size / 2) - (topUserNum / 2)),(int) Math.ceil(size / 2) + (topUserNum / 2)));
    }

    private static List<String> getGroupHashtagList(int groupNum) {
        List<String> hashtagList = new ArrayList<>();
        if(localRun){
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

    public static double calcConditionEntropy(){
        return 0;
    }

    private static void output(DataFrame data, String folderName, boolean flag) {
        if(flag)
            data.write().mode(SaveMode.Overwrite).format("com.databricks.spark.csv").save(outputPath + folderName + "_csv");
        data.write().mode(SaveMode.Overwrite).parquet(outputPath + folderName + "_parquet");
    }

    private static JavaRDD<Row> calcProb(final int groupNum, final boolean containFlag, final int fromUserCond, final double tweetNum){
        final List<String> hashtagList = getGroupHashtagList(groupNum);
        //System.out.println("FromProb: " + fromUserProb.limit(1).select("fromProb").head().getDouble(0));
        if(fromUserCond < 3) {
            return tweet_user_hashtag_grouped.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
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
                    String username = stringDoubleTuple2._1();
                    //double fromUserProb = sqlContext.sql("select fromProb from fromUserProb fb where fb.username = '" + username+"'").head().getDouble(0);
                    if(tweetNum == 0)
                        System.err.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                    return RowFactory.create(username, stringDoubleTuple2._2() / tweetNum);
                }
            });

        }else {
            return tweet_user_hashtag_grouped.drop("username").javaRDD().mapToPair(new PairFunction<Row, Integer, Long>() {
                @Override
                public Tuple2<Integer, Long> call(Row row) throws Exception {
                    List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(1).split(","))));
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

            //return RowFactory.create(row.getString(0), sqlContext.sql("select sum(prob) from condEntropyTweetTrueFromUserTrue where username <> " + row.getString(0)).head().getDouble(0));
            //return RowFactory.create(row.getString(0), sqlContext.sql("select sum(prob) from condEntropyTweetFalseFromUserTrue where username <> " + row.getString(0)).head().getDouble(0));
        }
    }

    private static JavaRDD<Row> calcHashtagProb(final int groupNum, final boolean containFlag, final int fromUserCond, final double tweetNum){
        final List<String> hashtagList = getGroupHashtagList(groupNum);
        //System.out.println("FromProb: " + fromUserProb.limit(1).select("fromProb").head().getDouble(0));
        if(fromUserCond < 3) {
            //TODO FIX IT
            return tweet_hashtag_hashtag_grouped.javaRDD().mapToPair(new PairFunction<Row, String, Double>() {
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
                    String username = stringDoubleTuple2._1();
                    //double fromUserProb = sqlContext.sql("select fromProb from fromUserProb fb where fb.username = '" + username+"'").head().getDouble(0);
                    if(tweetNum == 0)
                        System.err.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                    return RowFactory.create(username, stringDoubleTuple2._2() / tweetNum);
                }
            });

        }else {
            return tweet_hashtag.javaRDD().mapToPair(new PairFunction<Row, Integer, Long>() {
                @Override
                public Tuple2<Integer, Long> call(Row row) throws Exception {
                    List<String> tH = new ArrayList<String>(Arrays.asList((row.getString(1).split(","))));
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

            //return RowFactory.create(row.getString(0), sqlContext.sql("select sum(prob) from condEntropyTweetTrueFromUserTrue where username <> " + row.getString(0)).head().getDouble(0));
            //return RowFactory.create(row.getString(0), sqlContext.sql("select sum(prob) from condEntropyTweetFalseFromUserTrue where username <> " + row.getString(0)).head().getDouble(0));
        }
    }

    public static void calcTweetCondFromHashtagConditionalEntropy(final int groupNum){
        StructField[] fields = {
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
        };
        //TODO should I cache some of the user probabilities in the memory
        //==============================================================================================================
        JavaRDD<Row> containNotContainNum = calcHashtagProb(groupNum, true, 3, tweetCount);
        StructField[] fields1 = {
                DataTypes.createStructField("key", DataTypes.IntegerType, true),
                DataTypes.createStructField("num", DataTypes.LongType, true),
        };
        (sqlContext.createDataFrame(containNotContainNum, new StructType(fields1))).registerTempTable("containNumTable");
        long containNum = sqlContext.sql("select num from containNumTable where key = 1").head().getLong(0);
        long notContainNum = sqlContext.sql("select num from containNumTable where key = 2").head().getLong(0);
        final double probTweetContain = (double)containNum / tweetCount;
        final double probTweetNotContain = (double)notContainNum / tweetCount;

        JavaRDD<Row> condEntropyTweetTrueFromHashtagTrue = calcHashtagProb(groupNum, true, 1, tweetCount);
        DataFrame results2 = sqlContext.createDataFrame(condEntropyTweetTrueFromHashtagTrue.coalesce(numPart), new StructType(fields));
        results2.registerTempTable("condEntropyTweetTrueFromHashtagTrue");

        DataFrame fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        JavaRDD<Row> res = fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        });
        JavaRDD<Row> resMI = fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(probTweetContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * row.getDouble(2))));
            }
        });
        System.out.println("SIZE 1=================" + resMI.count() + "================" );
        //==============================================================================================================
        JavaRDD<Row> condEntropyTweetFalseFromHashtagTrue = calcHashtagProb(groupNum, false, 2, tweetCount);
        results2 = (sqlContext.createDataFrame(condEntropyTweetFalseFromHashtagTrue.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseFromHashtagTrue");
        fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (probTweetNotContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * row.getDouble(2))));
            }
        }));
        System.out.println("SIZE 2=================" + resMI.count() + "================");
        //==============================================================================================================


        results2 = sqlContext.sql("select hashtag, (" + containNum + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetTrueFromHashtagTrue");
        fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (1-row.getDouble(2))));
            }
        }));
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 3=================" + resMI.count() + "================");
        //==============================================================================================================

        results2 = sqlContext.sql("select hashtag, (" + notContainNum + " - (prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long) tweetCount) + " AS prob from condEntropyTweetFalseFromHashtagTrue");
        fromresults2 = results2.join(fromHashtagProb, fromHashtagProb.col("hashtag1").equalTo(results2.col("hashtag"))).drop("hashtag1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (1 - row.getDouble(2))));
            }
        }));
        resMI = resMI.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (probTweetNotContain == 0)
                    System.out.println("NOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / (probTweetNotContain * (1-row.getDouble(2)))));
            }
        }));
        System.out.println("SIZE 4=================" + resMI.count() + "================");
        sqlContext.createDataFrame(res.coalesce(numPart), new StructType(fields)).registerTempTable("condEntropyTweetFromHashtag");
        sqlContext.createDataFrame(resMI.coalesce(numPart), new StructType(fields)).registerTempTable("mutualEntropyTweetFromHashtag");

        //======================== COMPUTE COND ENTROPY=================================================================
        fromresults2 = sqlContext.sql("SELECT hashtag, -sum(prob) AS condEntropy FROM condEntropyTweetFromHashtag GROUP BY hashtag");
        fromresults2 = fromresults2.sort(fromresults2.col("condEntropy"));//.limit((int) fromSize / 2 + topUserNum / 2);
        fromresults2.cache();
        //output(sqlContext.createDataFrame(takeMiddle(fromresults2, sparkContext), new StructType(fields)), "CondEntropyTweetFromUser_"+groupNum+"_middle", false);
        //output(fromresults2.limit(topUserNum), "CondEntropyTweetFromUser_"+groupNum+"_top", false);
        output(fromresults2.coalesce(1), "CondEntropyTweetFromHashtag", false);



        fromresults2 = sqlContext.sql("SELECT hashtag, sum(prob) AS mutualEntropy FROM mutualEntropyTweetFromHashtag GROUP BY hashtag");
        fromresults2 = fromresults2.sort(fromresults2.col("mutualEntropy"));
        //output(sqlContext.createDataFrame(takeMiddle(fromresults2, sparkContext), new StructType(fields)), "mutualEntropyTweetFromUser_"+groupNum+"_middle", false);
        //output(fromresults2.limit(topUserNum), "mutualEntropyTweetFromUser_"+groupNum+"_top", false);
        output(fromresults2.coalesce(1), "mutualEntropyTweetFromHashtag", false);

    }

}
