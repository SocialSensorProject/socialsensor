package edu.oregonstate.spark.sensor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class GroupLagTime implements Serializable {
    private static String hdfsPath =
            "hdfs://ec2-54-172-144-54.compute-1.amazonaws.com:9000/";
    private static String dataPath = hdfsPath + "zaradata/input/input/";
    private static String outputPath = hdfsPath + "zaradata/output/";
    private static Random randObj = new Random();
    public static void main(String args[]) throws Exception {
        System.out.println("=====================1===========================");
        SparkConf sparkConfig = new SparkConf().setAppName("LagTime");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);

        // read data from parquet files
        DataFrame user_hashtag_birthday = sqlContext.read().parquet(dataPath + "user_hashtag_birthday_parquet");
        DataFrame user_mention = sqlContext.read().parquet(dataPath + "user_mention_parquet");
        DataFrame tweet_user = sqlContext.read().parquet(dataPath + "tweet_user_parquet");
        DataFrame tweet_hashtag_time = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet");

        /************** prepare tables ***********/
        //long firstBday = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy").parse("Thu Feb 28 00:00:00 +0000 2013").getTime();
        user_hashtag_birthday.registerTempTable("user_hashtag_birthday");
        tweet_user.distinct().registerTempTable("tweet_user");
        tweet_hashtag_time.registerTempTable("tweet_hashtag_time");

        sqlContext.sql("select hashtag, min(uhb.birthday) AS birthday from user_hashtag_birthday uhb" +
        " GROUP BY hashtag HAVING (COUNT(username) BETWEEN 2500 AND 500000) AND (min(uhb.birthday) > 1362009600000)").registerTempTable("hashtag_birthday_nbrUsers");

        sqlContext.cacheTable("hashtag_birthday_nbrUsers");

        user_hashtag_birthday = sqlContext.sql("select uhb.username, uhb.hashtag, uhb.birthday from user_hashtag_birthday uhb, hashtag_birthday_nbrUsers hb where uhb.hashtag = hb.hashtag");
        user_hashtag_birthday.cache();
        user_hashtag_birthday.registerTempTable("user_hashtag_birthday");
        sqlContext.cacheTable("user_hashtag_birthday");

        // Pre-compute this for fast computation: long tweetCount = 829026458;
        long tweetCount = tweet_user.drop("username").distinct().count();


        // This table is built in order to creaste sensor group from control group later
        // Spark does not support collect_set or group_concat, the only way is to use
        // Map-Reduce
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

        // Key-Values are in different partition and need to be repartition as one
        JavaRDD<Row> usermentionRdd = userGroupMentions.coalesce(1).map(new Function<Tuple2<String, String>, Row>() {
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
        //user_mentione_grouped.registerTempTable("user_mention");
        user_mentione_grouped.cache();
        long user_mentioned1 = user_mentione_grouped.count();
        System.out.println("============================= user-mentioned: " + user_mentioned1 + "========================================");

        /*************Computing Lag times in multiple iterations***********/

        int numIter = 50;
        double cgSize = 1000.; //Integer.parseInt(args[1]);

        long time1, time2;
        for (int i = 0; i < numIter; ++i) {
            time1  = System.currentTimeMillis();
            System.err.println("======================================" + i + "=============================================");
            DataFrame CG = user_mentione_grouped.sample(false, cgSize / user_mentioned1);
            CG.drop("mentionee").registerTempTable("CG");

            JavaRDD<Row> BRows = CG.javaRDD().map(
                    new Function<Row, Row>() {
                        public Row call(Row row) throws Exception {
                            String[] mentionsList = row.get(1).toString().split(",");
                            return RowFactory.create(mentionsList[randObj.nextInt(mentionsList.length)]);
                        }
                    });
            StructField[] fields2 = {
                    DataTypes.createStructField("username", DataTypes.StringType, true),
            };
            DataFrame SG = sqlContext.createDataFrame(BRows, new StructType(fields2));
            SG.distinct().registerTempTable("SG");

            //DataFrame A = sqlContext.sql("select tid from tweet_user tu inner join CG cg on tu.username = cg.username");
            //Compute N and M
            long A_size = sqlContext.sql("select count(tid) from tweet_user tu inner join CG cg on tu.username = cg.username").head().getLong(0);

            DataFrame B = sqlContext.sql("select tid, sg.username from tweet_user as tu inner join SG as sg on tu.username = sg.username");
            B.cache();
            long B_size = B.count();
            System.err.println("-------------------------------------------Iteration: " + i + " - A_Size: " + A_size + " - B_Size: " + B_size);


            DataFrame C = tweet_user.sample(false, (B_size) / tweetCount);
            DataFrame D;
            if(A_size > B_size)// This is because when I test in small dataset on my local laptop this can happen
                D = SG;
            else
                D = B.sample(false, (A_size) / B_size);

            C.registerTempTable("C");
            D.registerTempTable("D");
            //-------------------------- COMPUTE LAG-TIMES ------------------------------------
            // NEEDED to save in diffferent parquet files so it still can be distributed
            System.err.println("=========================START COMPUTING LAG TIME===============================");
            sqlContext.sql("SELECT uhb.hashtag, min(birthday) AS birthday from user_hashtag_birthday uhb" +
                    " inner join CG on uhb.username = CG.username group by hashtag").registerTempTable("firstUsage");
            DataFrame partRes = sqlContext.sql(
                    "SELECT hb.hashtag, COALESCE(f.birthday, 1420070399000) - hb.birthday AS Lag_A" +
                            " FROM hashtag_birthday_nbrUsers AS hb" +
                            " LEFT OUTER JOIN firstUsage AS f ON hb.hashtag = f.hashtag");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_A_" + i + "_csv");

            sqlContext.sql("SELECT uhb.hashtag, min(birthday) AS birthday from user_hashtag_birthday uhb" +
                    " inner join SG on uhb.username = SG.username group by hashtag").registerTempTable("firstUsage");
            partRes = sqlContext.sql(
                    "SELECT hb.hashtag, COALESCE(f.birthday, 1420070399000) - hb.birthday AS Lag_B" +
                            " FROM hashtag_birthday_nbrUsers AS hb" +
                            " LEFT OUTER JOIN firstUsage AS f ON hb.hashtag = f.hashtag");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_B_" + i + "_csv");

            //NOTE C & D are tweet based, so, you cannot use user-hashtag-birthday table

            sqlContext.sql("SELECT tht.hashtag, min(tht.time) AS birthday from C inner join tweet_hashtag_time tht" +
                    " on tht.tid = C.tid group by hashtag").registerTempTable("firstUsage");
            partRes = sqlContext.sql(
                    "SELECT hb.hashtag, COALESCE(f.birthday, 1420070399000) - hb.birthday AS Lag_C" +
                            " FROM hashtag_birthday_nbrUsers AS hb" +
                            " LEFT OUTER JOIN firstUsage AS f ON hb.hashtag = f.hashtag");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_C_" + i + "_csv");

            sqlContext.sql("SELECT tht.hashtag, min(tht.time) AS birthday from tweet_hashtag_time tht" +
                    " inner join D on tht.tid = D.tid group by hashtag").registerTempTable("firstUsage");
            partRes = sqlContext.sql(
                    "SELECT hb.hashtag, COALESCE(f.birthday, 1420070399000) - hb.birthday AS Lag_D" +
                            " FROM hashtag_birthday_nbrUsers AS hb" +
                            " LEFT OUTER JOIN firstUsage AS f ON hb.hashtag = f.hashtag");
            sqlContext.dropTempTable("firstUsage");
            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_D_" + i + "_csv");


            partRes.write().mode(SaveMode.Overwrite).parquet(outputPath + "result_" + i + "_csv");
            time2 = System.currentTimeMillis() - time1;
            System.err.println("**********************************Time Iteration:"+i+"*************************************: " + String.valueOf(TimeUnit.MILLISECONDS.toMinutes(time2)) + ", " + String.valueOf(TimeUnit.MILLISECONDS.toSeconds(time2) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time2))));
            sqlContext.dropTempTable("CG");
            sqlContext.dropTempTable("SG");
            sqlContext.dropTempTable("C");
            sqlContext.dropTempTable("D");
        }
    }
}