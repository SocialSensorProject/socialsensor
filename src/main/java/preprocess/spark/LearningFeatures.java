package preprocess.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import util.TweetUtil;

import java.io.IOException;
import java.util.List;

/**
 * Created by zahraiman on 1/28/16.
 */
public class LearningFeatures {
    private static boolean thousand = true;
    public static ConfigRead configRead;
    public static String outputPath;
    public static String dataPath;
    public static int groupNum;
    public static long[] timestamps;
    public static int numPart;
    public static String[] groupNames;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public LearningFeatures(String _outputPath, String _dataPath, int _groupNum, long[] _timestamps, int _numPart, String[] _groupNames) throws IOException {
        loadConfig();
        outputPath = _outputPath;
        dataPath = _dataPath;
        groupNum = _groupNum;
        groupNames = _groupNames;
        timestamps = _timestamps;
        numPart = _numPart;
    }

    public static void getGroupedFeaturesBaselineBased(SQLContext sqlContext, JavaSparkContext sc){
        TweetUtil tweetUtil = new TweetUtil();
        System.out.println("************************** " + dataPath + " "  + groupNum);
        StructField[] fieldsMention = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("mentionee", DataTypes.StringType, true)
        };
        StructField[] fieldsFrom = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("user", DataTypes.StringType, true)
        };
        StructField[] fieldsHashtag = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("hashtag", DataTypes.StringType, true)
        };
        StructField[] fieldsTerm = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("term", DataTypes.StringType, true)
        };
        StructField[] fieldsLocation = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("location", DataTypes.StringType, true)
        };
        StructField[] tweetTimeField = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.StringType, true)
        };


        //from, hashtag, mention, term, location
        List<List<String>> features = tweetUtil.getTopFeatures(groupNum, configRead.isLocal(), thousand);
        final List<String> fromFeatures = features.get(0);
        final List<String> hashtagFeatures = features.get(1);
        final List<String> mentionFeatures = features.get(2);
        final List<String> termFeatures = features.get(3);
        final List<String> locationFeatures = features.get(4);

        String folderName = "baselineFeatures1000/";
        if(!thousand)
            folderName = "baselineFeatures5000/";
        folderName = "";

        long ind = 1;
        DataFrame df2, df1;
        //DataFrame tweetTime = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        //tweetTime.cache();
        //System.out.println("======================= TWEET TIME COUNT =:" + tweetTime.count() + ":====================================");

        df1 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_user_parquet")
                .coalesce(numPart).javaRDD()
                .filter(new Function<Row, Boolean>() {
                            @Override
                            public Boolean call(Row v1) throws Exception {
                                return fromFeatures.contains(v1.getString(1));
                            }
                        }
                ), new StructType(fieldsFrom));
        ind = df1.count();
        System.out.println("******************************TWEET USER THRESHOLDED COUNT: " + ind);
        //df1 = df1.join(tweetTime, df1.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweetUtil.output(df1, folderName + "tweet_thsh_fromFeature_grouped", false, outputPath);
        System.err.println("================== IND VALUE AFTER FROM_FEATURE=================: " + ind);

        df1 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet")
                .drop("hashtag").distinct().coalesce(numPart).javaRDD()
                .filter(new Function<Row, Boolean>() {
                            @Override
                            public Boolean call(Row v1) throws Exception {
                                return termFeatures.contains(v1.getString(1));
                            }
                        }
                ), new StructType(fieldsTerm));

        long ind3 = df1.count();
        System.out.println("******************************TWEET TTERM THRESHOLDED COUNT: " + ind3);
        ind += ind3;
        //df1 = df1.join(tweetTime, df1.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweetUtil.output(df1, folderName + "tweet_thsh_termFeature_grouped", false, outputPath);
        System.err.println("================== IND VALUE AFTER TERM_FEATURE=================: " + ind);


        df1 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet")
                .drop("hashtagGrouped").distinct().coalesce(numPart).javaRDD()
                .filter(new Function<Row, Boolean>() {
                            @Override
                            public Boolean call(Row v1) throws Exception {
                                return hashtagFeatures.contains(v1.getString(1));
                            }
                        }
                ), new StructType(fieldsHashtag));
        ind3 = df1.count();
        System.out.println("******************************TWEET HASHTAG THRESHOLDED COUNT: " + ind3);
        ind += ind3;
        System.err.println("================== IND VALUE AFTER HASHTAG_FEATURE=================: " + ind);
        //df1 = df1.join(tweetTime, df1.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweetUtil.output(df1, folderName + "tweet_thsh_hashtagFeature_grouped", false, outputPath);

        final long ind4 = ind;
        //System.err.println("==========FINAL HASHTAG COUNT============= " + df2.count());


        df1 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet")
                .drop("hashtag").coalesce(numPart).javaRDD()
                .filter(new Function<Row, Boolean>() {
                            @Override
                            public Boolean call(Row v1) throws Exception {
                                return mentionFeatures.contains(v1.getString(1));
                            }
                        }
                ), new StructType(fieldsMention));
        ind3 = df1.count();
        System.out.println("******************************TWEET MENTION THRESHOLDED COUNT: " + ind3);
        ind += ind3;
        System.err.println("================== IND VALUE AFTER MENTION =================: " + ind);
        //df1 = df1.join(tweetTime, df1.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweetUtil.output(df1, folderName + "tweet_thsh_mentionFeature_grouped", false, outputPath);


        df2 = sqlContext.read().parquet(dataPath + "tweet_user_parquet").coalesce(numPart);
        df1 = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "user_location_clean.csv").coalesce(numPart);
        df2 = df2.join(df1, df2.col("username").equalTo(df1.col("C0"))).drop(df2.col("username")).drop(df1.col("C0"));//tid, location
        df2.printSchema();
        df2.show();
        df1 = sqlContext.createDataFrame(df2.javaRDD()
                .filter(new Function<Row, Boolean>() {
                            @Override
                            public Boolean call(Row v1) throws Exception {
                                return locationFeatures.contains(v1.getString(1));
                            }
                        }
                ), new StructType(fieldsLocation));
        ind3 = df1.count();
        System.out.println("******************************TWEET LOCATION THRESHOLDED COUNT: " + ind3);
        ind += ind3;
        //df1 = df1.join(tweetTime, df1.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
        tweetUtil.output(df1, folderName + "tweet_thsh_locationFeature_grouped", false, outputPath);
        System.err.println("==========FINAL Feature COUNT============= " + (ind - 1));
    }
}
