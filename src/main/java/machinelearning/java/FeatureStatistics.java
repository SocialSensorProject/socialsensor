package machinelearning.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import preprocess.spark.ConfigRead;
import scala.Tuple2;

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
    private static boolean testSet = false;
    private static int topUserNum;
    private static boolean localRun;
    private static ConfigRead configRead;


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
        int groupNum = 1;
        initializeSqlContext();
        calcFromUserProb(tweetCount);
        calcToUserProb(tweetCount);
        calcTweetCondFromUserConditionalEntropy(groupNum);
    }

    public static void initializeSqlContext(){
        SparkConf sparkConfig;
        if(localRun) {
            dataPath = configRead.getTestDataPath();
            outputPath = configRead.getTestOutPath();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics").setMaster("local[2]").set("spark.executor.memory", "6g");
        }else {
            tweetCount = 829026458; //tweet_user.count();
            sparkConfig = new SparkConf().setAppName("FeatureStatistics");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);
        tweet_user = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);
        tweet_user.cache();
        tweet_user_hashtag_grouped = sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").coalesce(numPart);//.registerTempTable("tweet_user_hashtag_grouped");
        tweet_user_hashtag_grouped.cache();
        System.out.println(" HAS READ THE TWEET_HASHTAG ");
        if(localRun)
            tweetCount = tweet_user.count();//TODO change hardcode numbers
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
                return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2() / 829026458);
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("username1", DataTypes.StringType, true),
                DataTypes.createStructField("fromProb", DataTypes.DoubleType, true),
        };
        //userFromProbMap = userFromProb.collectAsMap();
        fromUserProb = sqlContext.createDataFrame(userFromProb, new StructType(fields));
        fromUserProb.show();
        fromUserProb.registerTempTable("fromUserProb");
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
        System.out.println("SIZE 1=================" + res.count() + "================");

        DataFrame toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        JavaRDD<Row> tores = toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1)/row.getDouble(2)));
            }
        });
        System.out.println("SIZE 1 TO ================="+ tores.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> condEntropyTweetFalseFromUserTrue = calcProb(groupNum, false, 2, tweetCount);
        results2 = (sqlContext.createDataFrame(condEntropyTweetFalseFromUserTrue.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseFromUserTrue");
        fromresults2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        res = res.union(fromresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        System.out.println("SIZE 2=================" + res.count() + "================");
        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        tores = tores.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        System.out.println("SIZE 2 to=================" + tores.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> containNotContainNum = calcProb(groupNum, true, 3, tweetCount);
        StructField[] fields1 = {
                DataTypes.createStructField("key", DataTypes.IntegerType, true),
                DataTypes.createStructField("num", DataTypes.LongType, true),
        };
        (sqlContext.createDataFrame(containNotContainNum, new StructType(fields1))).registerTempTable("containNumTable");
        Long containNum = sqlContext.sql("select num from containNumTable where key = 1").head().getLong(0);
        results2 = sqlContext.sql("select username, (" + containNum + "-(prob*" + BigInteger.valueOf((long) tweetCount) + "))/" + BigInteger.valueOf((long)tweetCount) + " AS prob from condEntropyTweetTrueFromUserTrue");
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
        System.out.println("SIZE 3=================" + res.count() + "================");
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
        System.out.println("SIZE 3 to =================" + tores.count() + "================");
        //==============================================================================================================
        Long notContainNum = sqlContext.sql("select num from containNumTable where key = 2").head().getLong(0);
        results2 = sqlContext.sql("select username, (" + notContainNum + " - (prob*" + BigInteger.valueOf((long)tweetCount) + "))/" + BigInteger.valueOf((long)tweetCount) + " AS prob from condEntropyTweetFalseFromUserTrue");
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
        System.out.println("SIZE 4=================" + res.count() + "================");
        sqlContext.createDataFrame(res.coalesce(numPart), new StructType(fields)).registerTempTable("condEntropyTweetFromUser");

        toresults2 = results2.join(toUserProb, toUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        System.out.println("*****^^^^^^^^^TO^^^^^^^^^^^^^^*******");
        toresults2.filter(toresults2.col("username").equalTo("paigeaucoin")).show();
        tores = tores.union(toresults2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        System.out.println("SIZE 4 to=================" + tores.count() + "================");
        sqlContext.createDataFrame(tores.coalesce(numPart), new StructType(fields)).registerTempTable("condEntropyTweetToUser");

        //======================== COMPUTE COND ENTROPY=================================================================
        fromresults2 = sqlContext.sql("SELECT username, -sum(prob) AS condEntropy FROM condEntropyTweetFromUser GROUP BY username");
        fromresults2 = fromresults2.sort(fromresults2.col("condEntropy"));
        fromresults2.limit(topUserNum).show();
        //output(results2.limit(topUserNum), "CondEntropyTweetFromUser_top10", false);
        output(fromresults2, "CondEntropyTweetFromUser", false);

        toresults2 = sqlContext.sql("SELECT username, -sum(prob) AS condEntropy FROM condEntropyTweetToUser GROUP BY username");
        toresults2 = toresults2.sort(toresults2.col("condEntropy"));
        toresults2.limit(topUserNum).show();
        //output(results2.limit(topUserNum), "CondEntropyTweetFromUser_top10", false);
        output(toresults2, "CondEntropyTweetToUser", false);


        //==============================================================================================================
        /*sqlContext.sql("select sum(t1.prob, t2.prob, t3.prob, t4.prob) from condEntropyTweetTrueFromUserTrue t1  INNER JOIN " +
                "condEntropyTweetFalseFromUserTrue t2 on t1.username = t2.username  INNER JOIN" +
                "condEntropyTweetTrueFromUserFalse t3 on t2.username = t3.username  INNER JOIN" +
                "condEntropyTweetFalseFromUserFalse t4 on t3.username = t4.username");*/
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
            else if (groupNum == 2)
                hashtagStrList = "()";
            else if (groupNum == 3)
                hashtagStrList = "()";
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
                    return RowFactory.create(username, stringDoubleTuple2._2() / 829026458);
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

}
