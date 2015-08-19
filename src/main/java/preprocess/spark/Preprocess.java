package preprocess.spark;


import com.twitter.Extractor;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

public class Preprocess implements Serializable {



    private static String hdfsPath;
    private static String dataPath; //"TestSet/";
    private static String outputPath; // "TestSet/output_all/";
    private static ConfigRead configRead;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }
    private static Extractor hmExtractor = new Extractor();
    private static int numPart;

    public static void main(String args[]) throws IOException {
        loadConfig();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        System.out.println("****************************** "+hdfsPath);
        dataPath = hdfsPath + configRead.getDataPath(); //configRead.getTestDataPath();
        outputPath = hdfsPath + configRead.getOutputPath(); //configRead.getLocalOutputPath()
        boolean local = configRead.isLocal();
        boolean tweetHashtagTime = configRead.isTweetHashtagTime();
        boolean uniqueUserHashtagBirthday = configRead.isUniqueUserHashtagBirthday();
        boolean directedUserNet = configRead.isDirectedUserNet();
        boolean groupedUserMention = configRead.isGroupedUserMention();
        boolean tweetUser = configRead.isTweetUser();
        boolean tweetUserHashtag = configRead.isTweetUserHashtag();
        boolean groupedTweetHashtag = configRead.isGroupedTweetHashtag();

        SparkConf sparkConfig;
        if(local) {
            numPart = 4;
            sparkConfig = new SparkConf().setAppName("SparkTest").setMaster("local[2]");
        }else {
            sparkConfig = new SparkConf().setAppName("SparkTest");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions="+numPart);



        DataFrame mainData = null;
        if(local) {
            mainData = sqlContext.read().json(dataPath + "testset1.json").coalesce(numPart);
        }else if(tweetHashtagTime || uniqueUserHashtagBirthday || directedUserNet || tweetUserHashtag ||tweetUser) {
            mainData = sqlContext.read().json(dataPath + "tweets2013-2014-v2.0*.bz2").coalesce(numPart);
        }
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
        if(groupedTweetHashtag) {
            getGroupedTweetHashtag(sqlContext);
            //getTweetMention(sqlContext.read().json(dataPath + "*.bz2").coalesce(3 * 16).select("id", "text"), sqlContext);
        }

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

    private static void getDirectedUserNetwork(DataFrame userTweets, SQLContext sqlContext){
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

    public static void getGroupedTweetHashtag(SQLContext sqlContext){
        System.out.println("************************** " + dataPath + "tweet_hashtag_time_parquet");
        JavaRDD<Row> t1 = sqlContext.read().parquet(dataPath + "tweet_hashtag_time_parquet").drop("time").coalesce(numPart).javaRDD().mapToPair(
                new PairFunction<Row, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Row row) throws Exception {
                        return new Tuple2(row.getLong(0), row.getString(1));
                    }
                })
                .reduceByKey(new Function2<String, String, String>() {
                    @Override
                    public String call(String s, String s2) throws Exception {
                        return s + "," + s2;
                    }
                })
                .map(new Function<Tuple2<Long, String>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, String> stringStringTuple2) throws Exception {
                        return RowFactory.create(stringStringTuple2._1(), stringStringTuple2._2());
                    }
                });

        StructField[] fields1 = {
                DataTypes.createStructField("tid1", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
        };
        DataFrame t = sqlContext.createDataFrame(t1, new StructType(fields1)).coalesce(numPart);
        t.cache();
        System.out.println("=================== t count: " + t.count());
        DataFrame tweet_user = sqlContext.read().parquet(dataPath + "tweet_user_parquet").drop("time").coalesce(numPart);
        tweet_user.cache();
        System.out.println("=================== tweet_user count: " + tweet_user.count());
        t = tweet_user.join(t, t.col("tid1").equalTo(tweet_user.col("tid"))).drop("tid1");
        System.out.println("======================= " + t.drop("username").drop("hashtag").count());
        t.write().mode(SaveMode.Overwrite).parquet(dataPath + "tweet_user_hashtag_grouped_parquet");
    }

}














