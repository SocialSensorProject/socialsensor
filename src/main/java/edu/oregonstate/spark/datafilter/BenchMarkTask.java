package edu.oregonstate.spark.datafilter;


import com.twitter.Extractor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class BenchMarkTask implements Serializable {
    private static String hdfsPath =
            "hdfs://ec2-54-172-144-54.compute-1.amazonaws.com:9000/";
    private static String dataPath = hdfsPath + "/zaradata/tweets2013-2014-v2.0/";//"zaradata/input/";
    private static String outputPath = hdfsPath + "zaradata/input/";

    private static Extractor hmExtractor = new Extractor();

    public static void main(String args[]) {
        long time1, time2;
        SparkConf sparkConfig = new SparkConf().setAppName("SparkTest");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        DataFrame mainData = sqlContext.read().json(dataPath + "*.bz2");
        // -----------------TWEET HASHTAG TIME--------------------------------------
        DataFrame x = mainData.select("id", "text", "created_at");
        getTweetHashtagTime(x, sqlContext);
        // ----------------- USER HASHTAG BIRTHDAY--------------------------------------
        x = mainData.select("screen_name", "text", "created_at");
        getUniqueUsersHashtagsAndBirthdays1(x, sqlContext);
        // ----------------- USER MENTION NETWORK--------------------------------------
        x = mainData.select("screen_name", "text");
        getDirectedUserNetwork(x, sqlContext);
        // ----------------- TWEET_ID USER--------------------------------------------
        x = mainData.select("id", "screen_name");
        getTweetUser(x, sqlContext);

        //mainData.saveAsParquetFile(outputPath + "mainData.parquet");
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
                            list.add(RowFactory.create(username, word.toLowerCase(), epochSec));
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
        //schemaUserHashtags.cache();
        output(schemaUserHashtags, "tweet_hashtag_time", false);
    }

    private static void getUniqueHashtags(DataFrame usersHashtagsTime, SQLContext sqlContext) {
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
                return RowFactory.create(row.getLong(0), row.get(1).toString());
            }
        });
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true)
        };
        tweet_user.cache();
        output(sqlContext.createDataFrame(tweet_user, new StructType(fields)), "tweet_user", false);
    }

    private static void getDirectedUserNetwork(DataFrame userTweets, SQLContext sqlContext){
        JavaRDD<Row> directed_user = userTweets.javaRDD().flatMap(
                new FlatMapFunction<Row, Row>() {
                    @Override
                    public Iterable<Row> call(Row row) throws Exception {
                        String username;
                        username = row.get(0).toString();
                        ArrayList<Row> list = new ArrayList<>();
                        for (String word : hmExtractor.extractMentionedScreennames(row.get(1).toString())) {
                            list.add(RowFactory.create(username, word));
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

}














