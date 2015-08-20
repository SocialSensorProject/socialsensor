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
import java.util.*;

/**
 * Created by zahraiman on 8/10/15.
 */
public class FeatureStatistics {
    private static String hdfsPath;
    private static int numPart;
    private static DataFrame tweet_user;
    private static long tweetCount;
    private static SQLContext sqlContext;
    private static DataFrame tweet_user_hashtag_grouped;
    private static DataFrame fromUserProb;
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
        calcFromUserProb();
        calcTweetCondFromUserConditionalEntropy(groupNum);
    }

    public static void initializeSqlContext(){
        SparkConf sparkConfig;
        if(localRun) {
            tweetCount = tweet_user.count();
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
    }

    public static void calcFromUserProb(){
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
                System.out.println(stringDoubleTuple2._1() + " ----- " + stringDoubleTuple2._2() + " : " +(double)stringDoubleTuple2._2()/tweetCount);
                if(Objects.equals(stringDoubleTuple2._1(), "paigeaucoin"))
                    System.out.println("LOOOOKKKKKKK: " + stringDoubleTuple2._2() + " " + tweetCount+ " " + (double) stringDoubleTuple2._2() / tweetCount);
                return RowFactory.create(stringDoubleTuple2._1(),  stringDoubleTuple2._2() / (double) tweetCount);
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("username1", DataTypes.StringType, true),
                DataTypes.createStructField("fromProb", DataTypes.DoubleType, true),
        };
        //userFromProbMap = userFromProb.collectAsMap();
        fromUserProb = sqlContext.createDataFrame(userFromProb, new StructType(fields));
        System.out.println("LOOOOKKKKKKK: " + tweetCount);
        fromUserProb.filter(fromUserProb.col("username1").equalTo("paigeaucoin")).show();
        //fromUserProb.registerTempTable("fromUserProb");
    }

    public static void calcToUserProb(){
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
                if(stringDoubleTuple2._1().equals("paigeaucoin"))
                    System.out.println("LOOOOKKKKKKK: " + stringDoubleTuple2._2() + " " + tweetCount+ " " + (double) stringDoubleTuple2._2() / tweetCount);
                return RowFactory.create(stringDoubleTuple2._1(), (double) stringDoubleTuple2._2() / tweetCount);
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("username1", DataTypes.StringType, true),
                DataTypes.createStructField("toProb", DataTypes.DoubleType, true),
        };
        DataFrame fromUserProb = sqlContext.createDataFrame(userToProb, new StructType(fields));
        fromUserProb.registerTempTable("toUserProb");
    }

    /*public static void calcTweetCondFromUserProb_V2(final int groupNum) {
        JavaRDD<Row> probTweetTrueCondFromUser = tweet_user.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String username = row.getString(1);
                double prob = 0.0, p1 = 0, p2 = 0, p3 = 0, p4 = 0;
                double numHashtags = sqlContext.sql("select count(*) from tweet_hashtag where tid = " + row.getLong(0) + " and hashtag IN " + getGroupHashtagList(groupNum)).head().getDouble(0);
                double fromUserProb = sqlContext.sql("select fromProb from fromUserProb where username = "+row.getString(1)).head().getDouble(0);
                if (numHashtags > 0) { // t contains h1
                    //p4 = Math.log(1.0 / fromUserProb);
                    prob += 1.0 / fromUserProb;
                } else {                // t does not contains h1
                    prob += 0.0;
                }
                return RowFactory.create(row.getLong(0), username, prob);
                //TODO do I need to assign P(ti | uj) for other j != i where it's definitely zero?!!
            }
        });
        JavaRDD<Row> probTweetFalseCondFromUser = tweet_user.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String username = row.getString(1);
                double numHashtags = sqlContext.sql("select count(*) from tweet_hashtag where tid = " + row.getLong(0) + " and hashtag IN " + getGroupHashtagList(groupNum)).head().getDouble(0);
                double fromUserProb = sqlContext.sql("select fromProb from fromUserProb where username = " + row.getString(1)).head().getDouble(0);
                if (numHashtags == 0)
                    return RowFactory.create(row.getLong(0), username, 1.0 / fromUserProb);
                else
                    return RowFactory.create(row.getLong(0), username, 0.0);
                //TODO do I need to assign P(ti | uj) for other j != i where it's definitely zero?!!
            }
        });
    }
    */

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
        JavaRDD<Row> condEntropyTweetTrueFromUserTrue = calcProb(groupNum, true, 1);
        DataFrame results2 = sqlContext.createDataFrame(condEntropyTweetTrueFromUserTrue.coalesce(numPart), new StructType(fields));

        results2.registerTempTable("condEntropyTweetTrueFromUserTrue");
        results2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        JavaRDD<Row> res = results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getString(0).compareTo("paigeaucoin") == 0)
                    System.out.println("LOOOOKKKKKKK: " + row.getDouble(1) + " " + row.getDouble(2) + " " +  row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
                if(row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1)/row.getDouble(2)));
            }
        });
        System.out.println("SIZE 1================="+ res.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> condEntropyTweetFalseFromUserTrue = calcProb(groupNum, false, 2);
        results2 = (sqlContext.createDataFrame(condEntropyTweetFalseFromUserTrue.coalesce(numPart).distinct(), new StructType(fields)));
        results2.registerTempTable("condEntropyTweetFalseFromUserTrue");
        results2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        res = res.union(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getString(0).compareTo("paigeaucoin") == 0)
                    System.out.println("LOOOOKKKKKKK: " + row.getDouble(1) + " " + row.getDouble(2) + " " +  row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
                if(row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        System.out.println("SIZE 2=================" + res.count() + "================");
        //==============================================================================================================
        JavaRDD<Row> containNotContainNum = calcProb(groupNum, true, 3);
        StructField[] fields1 = {
                DataTypes.createStructField("key", DataTypes.IntegerType, true),
                DataTypes.createStructField("num", DataTypes.LongType, true),
        };
        (sqlContext.createDataFrame(containNotContainNum, new StructType(fields1))).registerTempTable("containNumTable");
        Long containNum = sqlContext.sql("select num from containNumTable where key = 1").head().getLong(0);
        results2 = sqlContext.sql("select username, (" + containNum + "-(prob*" + tweetCount + "))/" + tweetCount + " AS prob from condEntropyTweetTrueFromUserTrue");
        results2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        res = res.union(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getString(0).compareTo("paigeaucoin") == 0)
                    System.out.println("LOOOOKKKKKKK: " + row.getDouble(1) + " " + row.getDouble(2) + " " +  row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));
        System.out.println("SIZE 3=================" + res.count() + "================");
        //==============================================================================================================
        Long notContainNum = sqlContext.sql("select num from containNumTable where key = 2").head().getLong(0);
        results2 = sqlContext.sql("select username, (" + notContainNum + " - (prob*" + tweetCount + "))/" + tweetCount + " AS prob from condEntropyTweetFalseFromUserTrue");
        results2 = results2.join(fromUserProb, fromUserProb.col("username1").equalTo(results2.col("username"))).drop("username1");
        System.out.println("*****^^^^^^^^^^^^^^^^^^^^^^^*******");
        results2.filter(results2.col("username").equalTo("paigeaucoin")).show();
        res = res.union(results2.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                if(row.getString(0).compareTo("paigeaucoin") == 0)
                    System.out.println("LOOOOKKKKKKK: " + row.getDouble(1) + " " + row.getDouble(2) + " " +  row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
                if (row.getDouble(1) == 0 || row.getDouble(2) == 0)
                    return RowFactory.create(row.getString(0), 0.0);
                else
                    return RowFactory.create(row.getString(0), row.getDouble(1) * Math.log(row.getDouble(1) / row.getDouble(2)));
            }
        }));

        System.out.println("SIZE 4=================" + res.count() + "================");
        sqlContext.createDataFrame(res.coalesce(numPart), new StructType(fields)).registerTempTable("condEntropyTweetFromUser");
        results2 = sqlContext.sql("SELECT username, -sum(prob) AS condEntropy FROM condEntropyTweetFromUser GROUP BY username");
        results2 = results2.sort(results2.col("condEntropy"));
        results2.limit(topUserNum).show();
        output(results2, "CondEntropyTweetFromUser", false);


        //==============================================================================================================
        /*sqlContext.sql("select sum(t1.prob, t2.prob, t3.prob, t4.prob) from condEntropyTweetTrueFromUserTrue t1  INNER JOIN " +
                "condEntropyTweetFalseFromUserTrue t2 on t1.username = t2.username  INNER JOIN" +
                "condEntropyTweetTrueFromUserFalse t3 on t2.username = t3.username  INNER JOIN" +
                "condEntropyTweetFalseFromUserFalse t4 on t3.username = t4.username");*/
    }

    private static List<String> getGroupHashtagList(int groupNum) {
        List<String> hashtagList = new ArrayList<>();
        if(testSet){
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

    private static JavaRDD<Row> calcProb(final int groupNum, final boolean containFlag, final int fromUserCond){
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
                    return RowFactory.create(username, stringDoubleTuple2._2() / (double)tweetCount);
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
