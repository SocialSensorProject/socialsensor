package preprocess.spark;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import util.ConfigRead;
import util.TweetUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by zahraiman on 1/28/16.
 */
public class LearningMR {

    public static ConfigRead configRead;
    public static String outputPath;
    public static String dataPath;
    public static int groupNum;
    public static long[] timestamps;
    public static int numPart;
    public static String[] groupNames;

    private static int returnNum = 10000;
    private static final int topFeatureNum = 200000;
    private static long featureNum = 1000000;
    private static long sampleNum = 2000000;
    private static boolean thousand = true;
    private static final double featureNumWin = 1000;
    private static final boolean allInnerJoin = false;
    private static TweetUtil tweetUtil;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
        tweetUtil = new TweetUtil();
    }

    public LearningMR(String _outputPath, String _dataPath, int _groupNum, long[] _timestamps, int _numPart, String[] _groupNames) throws IOException {
        loadConfig();
        outputPath = _outputPath;
        dataPath = _dataPath;
        groupNum = _groupNum;
        groupNames = _groupNames;
        timestamps = _timestamps;
        numPart = _numPart;
    }


    public static void getLearningBaseline(SQLContext sqlContext) throws ParseException {
        boolean mixed = false;
        boolean NB = true;
        String alg = (mixed)? "Mixed" : "MixedLearning";
        alg = (NB)? "LearningMethods/NB" : alg;
        StructField[] fieldsFrom = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsFrom2 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true)
        };
        StructField[] fieldsMap = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsLocation = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("location", DataTypes.StringType, true)
        };

        DataFrame df1 = null, df2, df3 = null;
        DataFrame tweetTopical = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet").coalesce(numPart);


        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        final long splitTime = format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime();

        int returnNum = 10000;
        DataFrame topFeatures;


        DataFrame tweetTime = sqlContext.read().parquet(dataPath + "tweet_thsh_fromFeature_grouped_parquet").drop("username").coalesce(numPart);
        StructField[] timeField = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        final boolean testFlag2 = configRead.getTestFlag();
        tweetTime = sqlContext.createDataFrame(tweetTime.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                if (testFlag2)
                    return v1.getLong(1) > splitTime;
                else
                    return v1.getLong(1) > timestamps[groupNum - 1];
            }
        }), new StructType(timeField)).coalesce(numPart);
        tweetTime.cache();


        df2 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_thsh_fromFeature_grouped_parquet").drop("time").coalesce(numPart).javaRDD(), new StructType(fieldsFrom2));
        df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);


        String featureName = "featureWeights_from";
        System.out.printf("********************* " + dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        topFeatures = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        if(topFeatures.count() != 0) {
            topFeatures = sqlContext.createDataFrame(topFeatures.drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            System.out.println("********************* " + topFeatures.count());
            df2 = df1;
            df3 = topFeatures.join(df2, df2.col("username").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("username").coalesce(numPart);
            df1.registerTempTable("fromFeature");
            System.out.println("*****************************FROM : " + df3.count() + "**************************");
        }

        featureName = "featureWeights_hashtag";
        topFeatures = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        if(topFeatures.count() != 0) {
            topFeatures = sqlContext.createDataFrame(topFeatures.drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_hashtagFeature_grouped_parquet").drop("time").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
            df1 = topFeatures.join(df2, df2.col("hashtag").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("hashtag").coalesce(numPart);
            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));
            // SUM WITH THE PREVIOUS FEATURE
            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(3), v1.getDouble(2));
                        else if (v1.get(3) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(2));
                    }
                }), new StructType(fieldsMap)).coalesce(numPart);
            }else{
                df3 = df1;
            }
            System.out.println("*****************************HASHTAG : " + df3.count() + "**************************");
        }
        //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob) AS weight1 FROM fromFeature m1,hashtagFeature m2 on m1.tid = m2.tid").coalesce(numPart);
        //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum-1] + "/top1000Tweets1.csv");

        //hashtagFeat = hashtagFeat.sort(hashtagFeat.col("prob").desc()).limit(returnNum);
        //fromFeat = fromFeat.join(hashtagFeat, fromFeat.col("tid").equalTo(hashtagFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);

        featureName = "featureWeights_mention";
        topFeatures = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        if(topFeatures.count() > 0) {
            topFeatures = sqlContext.createDataFrame(topFeatures.drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_mentionFeature_grouped_parquet").drop("time").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
            df1 = topFeatures.join(df2, df2.col("mentionee").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("mentionee").coalesce(numPart);
            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));
            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(2), v1.getDouble(3));
                        else if (v1.get(2) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(3));
                    }
                }), new StructType(fieldsMap)).coalesce(numPart);
            }else
                df3 = df1;
            System.out.println("*****************************MENTION : " + df3.count() + "**************************");
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv");
            //mentionFeat = mentionFeat.sort(mentionFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(mentionFeat, mentionFeat.col("tid").equalTo(mentionFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
        }
        featureName = "featureWeights_term";
        topFeatures = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        if(topFeatures.count() > 0){
            topFeatures = sqlContext.createDataFrame(topFeatures.drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_termFeature_grouped_parquet").drop("time").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
            df1 = topFeatures.join(df2, df2.col("term").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("term").coalesce(numPart);
            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));

            df1.join(df2, df1.col("tid").equalTo(df2.col("tid"))).drop(df1.col("tid")).show(10000);
            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(2), v1.getDouble(3));
                        else if (v1.get(2) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(3));
                    }
                }), new StructType(fieldsMap)).coalesce(numPart);
            }else
                df3 = df1;
        }
        //System.out.println("*****************************TERM : " + df1.count() + "**************************");
        //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob) AS weight2 FROM mentionFeature m1,termFeature m2").coalesce(numPart);
        //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv");
        //termFeat = termFeat.sort(termFeat.col("prob").desc()).limit(returnNum);
        //fromFeat = fromFeat.join(termFeat, termFeat.col("tid").equalTo(termFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);


        //sqlContext.read().parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets1.csv").registerTempTable("table1");
        featureName = "featureWeights_location";
        topFeatures = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        if(topFeatures.count() > 0) {
            topFeatures = sqlContext.createDataFrame(topFeatures.drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_thsh_locationFeature_grouped_parquet").drop("time").javaRDD(), new StructType(fieldsLocation)).coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
            df1 = topFeatures.join(df2, df2.col("location").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("location").coalesce(numPart);
            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));
            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(2), -v1.getDouble(3));
                        else if (v1.get(2) == null)
                            return RowFactory.create(v1.getLong(0), -v1.getDouble(1));
                        else
                            return RowFactory.create(v1.getLong(0), -(v1.getDouble(1) + v1.getDouble(3)));
                    }
                }), new StructType(fieldsMap)).coalesce(numPart);
            }else
                df3 = df1;
        }
        //System.out.println("*****************************LOCATION : " + df1.count() + "**************************");
        //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.weight1) AS weight3 FROM locationFeature m1,table1 m2").coalesce(numPart);

        df2 = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "negative_train_parquet").coalesce(numPart);
        df1 = df3.join(df2, df3.col("tid").equalTo(df2.col("tid"))).drop(df2.col("tid"));
        if(mixed || NB)
            df1 = df1.sort(df1.col("prob").asc()).limit(returnNum);
        else
            df1 = df1.sort(df1.col("prob").desc()).limit(returnNum);
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets_noTrainTweet.csv");


        if(mixed || NB)
            df3 = df3.sort(df3.col("prob").asc()).limit(returnNum);
        else
            df3 = df3.sort(df3.col("prob").desc()).limit(returnNum);
        df1 = df3.join(tweetTopical, tweetTopical.col("tid").equalTo(df3.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);

        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets.csv");
        //locationFeat = locationFeat.sort(locationFeat.col("prob").desc()).limit(returnNum);
        //fromFeat = fromFeat.join(locationFeat, locationFeat.col("tid").equalTo(locationFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
        //fromFeat = fromFeat.sort(fromFeat.col("prob").desc()).limit(returnNum);

        //sqlContext.read().parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv").registerTempTable("table2");
        //sqlContext.read().parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets3.csv").registerTempTable("table3");
        //df1 = sqlContext.sql("SELECT m1.tid, (m1.weight2+m2.weight3) AS weight FROM table2 m1,table3 m2").coalesce(numPart);
        //df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets.csv");

        //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob+m3.prob+m4.prob+m5.prob) AS weight FROM fromFeature m1,hashtagFeature m2,mentionFeature m3,termFeature m4,locationFeature m5").coalesce(numPart);
        //df1.printSchema();
        //df1 = df1.sort(df1.col("weight").desc()).coalesce(numPart).limit(returnNum);
        //df1.show(20);
        //df1 = df1.join(tweetTopical, df1.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
        //df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum-1] + "/top1000Tweets.csv");
        //System.out.printf("********************* TOTAL SIZE 2: " + df3.count() + "***************************");
        System.out.println("==================== DONE======================");

    }

    public static void getRocchioLearning(SQLContext sqlContext) throws ParseException, IOException {
        String alg = "LearningMethods/Rocchio";
        StructField[] fieldsFrom = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsMap = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsResult = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true),
                DataTypes.createStructField("norm", DataTypes.DoubleType, true)
        };
        StructField[] fieldsLocation = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("location", DataTypes.StringType, true)
        };
        StructField[] fieldsNorm = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("norm", DataTypes.DoubleType, true)
        };

        DataFrame df1 = null, df2, df3 = null, dfNorm;
        DataFrame tweetTopical = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet").coalesce(numPart);


        final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
        final long splitTime = format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime();

        int returnNum = 10000;
        DataFrame topFeatures;

        final boolean testFlag2 = configRead.getTestFlag();
        DataFrame tweetTime = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);
        StructField[] timeField = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };

        tweetTime = sqlContext.createDataFrame(tweetTime.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                if (testFlag2)
                    return v1.getLong(1) > splitTime;
                else
                    return v1.getLong(1) > timestamps[groupNum - 1];
            }
        }), new StructType(timeField)).coalesce(numPart);
        tweetTime.cache();
//        System.out.println("TWEET TIME COUNT: " + tweetTime.count());
        FileReader fileReaderA;
        BufferedReader bufferedReaderA;
        boolean skipFeature = false;



        String featureName = "featureWeights_from";
        //dataPath = "/data/ClusterData/Output/BaselinesRes/Mixed/MI/";
        System.out.printf("********************* " + dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv" + "\n");
//        sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
//        fileReaderA = new FileReader(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
//        bufferedReaderA = new BufferedReader(fileReaderA);
//        skipFeature = (bufferedReaderA.readLine() == null);
        skipFeature = false;
        if(!skipFeature) {
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_fromFeature_grouped_parquet").coalesce(numPart);
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            df2 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
//            df2 = sqlContext.read().parquet(outputPath + "from_time_" + groupNum + "_parquet");;
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            System.out.println("********************* " + topFeatures.count());
            //df2 = df1;
            df3 = sqlContext.createDataFrame(topFeatures.join(df2, df2.col("username").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getLong(1), v1.getDouble(0), v1.getDouble(0)*v1.getDouble(0));
                }
            }), new StructType(fieldsResult));

            System.out.println("*****************************FROM : " + df3.count() + "**************************");
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile0");
            System.out.println("\nAFTER WRITING FROM");
            df2.unpersist();
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile0");
        }
//        bufferedReaderA.close();

        featureName = "featureWeights_hashtag";
//        fileReaderA = new FileReader(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
//        bufferedReaderA = new BufferedReader(fileReaderA);
//        skipFeature = (bufferedReaderA.readLine() == null);
        skipFeature = false;
        if(!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_hashtagFeature_grouped_parquet").coalesce(numPart);
            df2 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
//            df2 = sqlContext.read().parquet(outputPath + "hashtag_time_" + groupNum + "_parquet");
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            df1 = topFeatures.join(df2, df2.col("hashtag").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("hashtag").coalesce(numPart);

            dfNorm = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0) * row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsNorm));

            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));

            df1 = df1.join(dfNorm, df1.col("tid").equalTo(dfNorm.col("tid"))).drop(dfNorm.col("tid"));
            df1.printSchema();
            // SUM WITH THE PREVIOUS FEATURE
            if(df3 != null) {
                //tid, prob, norm, tid, prob, norm
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(3), v1.getDouble(4), v1.getDouble(5));
                        else if (v1.get(3) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1), v1.getDouble(2));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(4), v1.getDouble(2) + v1.getDouble(5));
                    }
                }), new StructType(fieldsResult)).coalesce(numPart);
                System.out.println("\n AFTER JOIN HASHTAG \n");
            }else
                df3 = df1;
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_"+alg+"_"+ groupNames[groupNum - 1] + "_tmpFile1");
            System.out.println("*****************************HASHTAG : " + df3.count() + "**************************");
            //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob) AS weight1 FROM fromFeature m1,hashtagFeature m2 on m1.tid = m2.tid").coalesce(numPart);
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum-1] + "/top1000Tweets1.csv");

            //hashtagFeat = hashtagFeat.sort(hashtagFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(hashtagFeat, fromFeat.col("tid").equalTo(hashtagFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile1");
        }
//        bufferedReaderA.close();
        System.out.println("\n HASHTAG DONE \n");


        featureName = "featureWeights_mention";
//        fileReaderA = new FileReader(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
//        bufferedReaderA = new BufferedReader(fileReaderA);
//        skipFeature = (bufferedReaderA.readLine() == null);
        skipFeature = false;
        if(!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/" + alg + "/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_mentionFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
//            df2 = sqlContext.read().parquet(outputPath + "Mention_time_" + groupNum + "_parquet");;
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            df1 = topFeatures.join(df2, df2.col("mentionee").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("mentionee").coalesce(numPart);

            dfNorm = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0) * row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsNorm));

            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));

            df1 = df1.join(dfNorm, df1.col("tid").equalTo(dfNorm.col("tid"))).drop(dfNorm.col("tid"));

            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(3), v1.getDouble(4), v1.getDouble(5));
                        else if (v1.get(3) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1), v1.getDouble(2));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(4), v1.getDouble(2) + v1.getDouble(5));
                    }
                }), new StructType(fieldsResult)).coalesce(numPart);
                System.out.println("\n AFTER JOIN MENTION \n");
            }else
                df3 = df1;
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile2");
            System.out.println("*****************************MENTION : " + df3.count() + "**************************");
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv");
            //mentionFeat = mentionFeat.sort(mentionFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(mentionFeat, mentionFeat.col("tid").equalTo(mentionFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile2");
        }
//        bufferedReaderA.close();

        df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile2");
        System.out.println("*****************************SIZE : " + df3.count() + "**************************");


        featureName = "featureWeights_term";
//        fileReaderA = new FileReader(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
//        bufferedReaderA = new BufferedReader(fileReaderA);
//        skipFeature = (bufferedReaderA.readLine() == null);
        skipFeature = false;
        if(!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_termFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
//            df2 = sqlContext.read().parquet(outputPath + "Term_time_" + groupNum + "_parquet");;
            //df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            df1 = topFeatures.join(df2, df2.col("term").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("term").coalesce(numPart);

            dfNorm = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0) * row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsNorm));

            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));

            df1 = df1.join(dfNorm, df1.col("tid").equalTo(dfNorm.col("tid"))).drop(dfNorm.col("tid"));
            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(3), v1.getDouble(4), v1.getDouble(5));
                        else if (v1.get(3) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1), v1.getDouble(2));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(4), v1.getDouble(2) + v1.getDouble(5));
                    }
                }), new StructType(fieldsResult)).coalesce(numPart);
            }else
                df3 = df1;
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile_Term");
            //System.out.println("*****************************TERM : " + df1.count() + "**************************");
            //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob) AS weight2 FROM mentionFeature m1,termFeature m2").coalesce(numPart);
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv");
            //termFeat = termFeat.sort(termFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(termFeat, termFeat.col("tid").equalTo(termFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
            //df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile_Term");
        }
//        bufferedReaderA.close();

        //df3 = sqlContext.read().parquet(outputPath+"LearningTweetWeights_"+alg+"_" + groupNames[groupNum - 1] + "_tmpFile_Term");
        //sqlContext.read().parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets1.csv").registerTempTable("table1");
        featureName = "featureWeights_location";
//        fileReaderA = new FileReader(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
//        bufferedReaderA = new BufferedReader(fileReaderA);
//        skipFeature = (bufferedReaderA.readLine() == null);
        skipFeature = false;
        if (!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
                @Override
                public Row call(Row v1) throws Exception {
                    return RowFactory.create(v1.getString(0).toLowerCase(), Double.valueOf(v1.getString(1)));
                }
            }).filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.getDouble(1) != 0.0;
                }
            }), new StructType(fieldsFrom)).coalesce(numPart);
            df2 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_thsh_locationFeature_grouped_parquet").javaRDD(), new StructType(fieldsLocation)).coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;
//            df2 = sqlContext.read().parquet(outputPath + "Location_time_" + groupNum + "_parquet");
            df2.printSchema();
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            df1 = topFeatures.join(df2, df2.col("location").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("location").coalesce(numPart);
            dfNorm = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0) * row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsNorm));

            df1 = sqlContext.createDataFrame(df1.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                @Override
                public Tuple2<Long, Double> call(Row row) throws Exception {
                    return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double a, Double b) throws Exception {
                    return a + b;
                }
            }).map(new Function<Tuple2<Long, Double>, Row>() {
                @Override
                public Row call(Tuple2<Long, Double> tuple) throws Exception {
                    return RowFactory.create(tuple._1(), tuple._2());
                }
            }), new StructType(fieldsMap));

            df1 = df1.join(dfNorm, df1.col("tid").equalTo(dfNorm.col("tid"))).drop(dfNorm.col("tid"));
            if(df3 != null) {
                df3 = sqlContext.createDataFrame(df1.join(df3, df1.col("tid").equalTo(df3.col("tid")), "outer").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row v1) throws Exception {
                        if (v1.get(0) == null)
                            return RowFactory.create(v1.getLong(3), v1.getDouble(4), v1.getDouble(5));
                        else if (v1.get(3) == null)
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1), v1.getDouble(2));
                        else
                            return RowFactory.create(v1.getLong(0), v1.getDouble(1) + v1.getDouble(4), v1.getDouble(2) + v1.getDouble(5));
                    }
                }), new StructType(fieldsResult)).coalesce(numPart);
            }else
                df3 = df1;
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_" +alg+"_"+
                    groupNames[groupNum - 1] + "_tmpFile_loc");
            //System.out.println("*****************************LOCATION : " + df1.count() + "**************************");
            //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.weight1) AS weight3 FROM locationFeature m1,table1 m2").coalesce(numPart);
        }
//        bufferedReaderA.close();

        df3 =  sqlContext.read().parquet(outputPath + "LearningTweetWeights_" +alg+"_"+ groupNames[groupNum - 1] + "_tmpFile_loc").coalesce(numPart);
        System.out.println("Final Matched Tweets Count: " + df3.count());

        df2 = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "negative_train_parquet").coalesce(numPart);
        df1 = df3.join(df2, df3.col("tid").equalTo(df2.col("tid"))).drop(df2.col("tid"));
        df1 = df1.sort(df1.col("prob").desc()).limit(returnNum);
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets_noTrainTweet.csv");

        df3 = df3.sort(df3.col("prob").desc()).limit(returnNum).coalesce(numPart);

        //df3.write().mode(SaveMode.Overwrite).parquet("LearningTweetWeights_Final_SortedLimited");
        df1 = df3.join(tweetTopical, tweetTopical.col("tid").equalTo(df3.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
        System.out.println("JOINED COUNT: " + df1.count());
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/"+alg+"/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets.csv");
        //locationFeat = locationFeat.sort(locationFeat.col("prob").desc()).limit(returnNum);
        //fromFeat = fromFeat.join(locationFeat, locationFeat.col("tid").equalTo(locationFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
        //fromFeat = fromFeat.sort(fromFeat.col("prob").desc()).limit(returnNum);

        //sqlContext.read().parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv").registerTempTable("table2");
        //sqlContext.read().parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets3.csv").registerTempTable("table3");
        //df1 = sqlContext.sql("SELECT m1.tid, (m1.weight2+m2.weight3) AS weight FROM table2 m1,table3 m2").coalesce(numPart);
        //df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets.csv");

        //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob+m3.prob+m4.prob+m5.prob) AS weight FROM fromFeature m1,hashtagFeature m2,mentionFeature m3,termFeature m4,locationFeature m5").coalesce(numPart);
        //df1.printSchema();
        //df1 = df1.sort(df1.col("weight").desc()).coalesce(numPart).limit(returnNum);
        //df1.show(20);
        //df1 = df1.join(tweetTopical, df1.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
        //df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum-1] + "/top1000Tweets.csv");
        //System.out.printf("********************* TOTAL SIZE 2: " + df3.count() + "***************************");
        System.out.println("==================== DONE======================");

    }

    /*private static void getTweetTopical(SQLContext sqlContext, boolean testTopical, boolean isTrain) throws IOException {
        //TODO just include test hashtags for the baselines
        List<String> hashtagListTmp;
        if(testTopical)
            hashtagListTmp = tweetUtil.getTestTrainGroupHashtagList(groupNum, localRun, isTrain);
        else
            hashtagListTmp =  tweetUtil.getGroupHashtagList(groupNum, localRun);
        final List<String> hashtagList = hashtagListTmp;
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("topical", DataTypes.IntegerType, true)
        };
        DataFrame df = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").drop("username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                if (v1.getString(1).equals(""))
                    return RowFactory.create(v1.getLong(0), 0);
                List<String> tH = new ArrayList<String>(Arrays.asList((v1.getString(1).split(","))));
                tH.retainAll(hashtagList);
                int numHashtags = tH.size();
                if (numHashtags > 0)
                    return RowFactory.create(v1.getLong(0), 1);
                else
                    return RowFactory.create(v1.getLong(0), 0);
            }
        }), new StructType(fields));
        if(!isTrain)
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_topical_" + groupNum + "_parquet");
        //DataFrame negativeSamples, positiveSamples;
        if(isTrain)
            df.filter(df.col("topical").$eq$eq$eq(0)).write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_topical_" + groupNum + "negative_train_parquet");
        // positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);

        //System.out.println("================== TWEET TOPICAL COUNT: " + df.count() + "========================");
        System.out.println("================== TWEET TOPICAL POSITIVE COUNT: " + df.filter(df.col("topical").$greater(0)).coalesce(numPart).count() + "========================");
    }*/

    public static void getTweetTopical(SQLContext sqlContext, boolean testTopical, final boolean haveNoTrain) throws IOException {
        final List<String> trainHashtags = tweetUtil.getTestTrainGroupHashtagList(groupNum, configRead.isLocal(), true);
        final List<String> testHashtags = tweetUtil.getTestTrainGroupHashtagList(groupNum, configRead.isLocal(), false);
        StructField[] fields = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("topical", DataTypes.IntegerType, true)
        };

        DataFrame df = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_user_hashtag_grouped_parquet").drop("username").coalesce(numPart).javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row v1) throws Exception {
                if (v1.getString(1).equals(""))
                    return RowFactory.create(v1.getLong(0), -1);
                List<String> hashtags = new ArrayList<String>(Arrays.asList(v1.getString(1).split(",")));
                List<String> hashtags2 = new ArrayList<String>();
                hashtags2.addAll(hashtags);
                hashtags.retainAll(trainHashtags);
                hashtags2.retainAll(testHashtags);
                int topical = (hashtags2.size() > 0) ? 1 : 0;
                if(haveNoTrain){
                    if(hashtags.size() > 0)
                        return RowFactory.create(v1.getLong(0), -1);
                    else
                        return RowFactory.create(v1.getLong(0), topical);
                }else{
                    if(hashtags.size() > 0 || hashtags2.size() > 0)
                        return RowFactory.create(v1.getLong(0), 1);
                    else
                        return RowFactory.create(v1.getLong(0), 0);
                }
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getInt(1) >= 0;
            }
        }), new StructType(fields));
        if(!haveNoTrain)
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_topical_" + groupNum + "_parquet");
        //DataFrame negativeSamples, positiveSamples;
        if(haveNoTrain)
            df.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_topical_" + groupNum + "negative_train_parquet");
        // positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);

        //System.out.println("================== TWEET TOPICAL COUNT: " + df.count() + "========================");
        System.out.println("================== TWEET TOPICAL COUNT: " + df.count() + "========================");
    }

    public static void getBaseline(SQLContext sqlContext) {
        StructField[] fieldsMention = {
                DataTypes.createStructField("mentionee", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsFrom = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsFrom2 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        StructField[] fieldsHashtag = {
                DataTypes.createStructField("hashtag", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsMap = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fields2 = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("feature", DataTypes.StringType, true)
        };
        StructField[] fieldsTerm = {
                DataTypes.createStructField("term", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] fieldsLocation = {
                DataTypes.createStructField("location", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
        };
        StructField[] qrelField = {
                DataTypes.createStructField("topicId", DataTypes.IntegerType, true),
                DataTypes.createStructField("Q0", DataTypes.StringType, true),
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("topical", DataTypes.IntegerType, true)
        };
        StructField[] qTopicsField = {
                DataTypes.createStructField("topicId", DataTypes.IntegerType, true),
                DataTypes.createStructField("Q0", DataTypes.StringType, true),
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("rank", DataTypes.LongType, true),
                DataTypes.createStructField("sim", DataTypes.StringType, true),
                DataTypes.createStructField("runId", DataTypes.StringType, true)
        };

        DataFrame df1 = null, df2, df3;
        DataFrame tweetTopical = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet").coalesce(numPart);
        DataFrame tweetTestTopical = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "negative_train_parquet").coalesce(numPart);


        DataFrame topFeatures;
        String[] algNames = {"topical", "topicalLog", "MILog", "CP", "CPLog", "MI"};

        DataFrame tweetTime = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);
        StructField[] timeField = {
                DataTypes.createStructField("tid", DataTypes.LongType, true),
                DataTypes.createStructField("time", DataTypes.LongType, true)
        };
        tweetTime = sqlContext.createDataFrame(tweetTime.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return v1.getLong(1) > timestamps[groupNum - 1];
            }
        }), new StructType(timeField)).coalesce(numPart);
        tweetTime.cache();

        boolean calcFrom = false, calcHashtag = false, calcTerm = true, calcMention = false, calcLocation = false;

        if (calcFrom) {
            //algNames = new String[]{"topical", "topicalLog", "MILog", "CP", "CPLog", "MI"};
            algNames = new String[]{"MI"};
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_fromFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);

            for (String algName : algNames) {
                final String featureName = "From";
                System.out.println("====================================== " + algName + " - " + featureName + "=======================================");
                //topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Baselines/" + groupNames[groupNum] + "/" + algName + "/top"+topFeatureNum+"_" + featureName + ".csv").javaRDD(), new StructType(fieldsFrom)).coalesce(numPart);
                System.out.println("*************" + outputPath + "Baselines/" + groupNames[groupNum - 1] + "/" + algName + "/top"+topFeatureNum+"_" + featureName + "_parquet");
                String ffName = "/top"+topFeatureNum+"_" + featureName + "_parquet";
                if (algName.contains("topical"))
                    ffName = "/top"+topFeatureNum+"_" + featureName;
                topFeatures = sqlContext.createDataFrame(sqlContext.read().parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/" + algName + ffName).javaRDD(), new StructType(fieldsFrom)).coalesce(numPart);
                System.out.println("===================== TOP FEATURES: " + topFeatures.count() + "==================");
                df2 = df1;
                df2 = topFeatures.join(df2, df2.col("username").equalTo(topFeatures.col("username"))).drop(df2.col("username")).coalesce(numPart);
                System.out.println("===================== TOP FEATURES JOIN FROM: " + df2.count() + "==================");
                df3 = df2.join(tweetTestTopical, df2.col("tid").equalTo(tweetTestTopical.col("tid"))).drop(tweetTestTopical.col("tid")).coalesce(numPart);
                df3 = df3.sort(df3.col("prob").desc()).limit(returnNum);
                df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_noTrainTweet_" + featureName);
                df2 = df2.sort(df2.col("prob").desc()).limit(returnNum);
                df2 = df2.join(tweetTopical, df2.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
                df2.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_TestTrain_" + featureName);
                df3 = null;
            }
            System.out.println("=====================FROM DONE======================");

        }
        if(calcHashtag){
            //algNames = new String[]{"CPLog", "MI", "topical", "topicalLog", "MILog", "CP"};
            algNames = new String[]{"MI"};
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_hashtagFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
            //System.out.println("============================ HASHTAG FEATURE : " + df2.count() + "===============================================");
            System.out.println("============================ HASHTAG FEATURE IN TEST SET: " + df1.count() + "===============================================");
            for (String algName : algNames) {
                final String featureName1 = "Hashtag";
                System.out.println("====================================== " + algName + " - " + featureName1 + "=======================================");
                String fName = "hashtag";
                String ffName = "/top"+topFeatureNum+"_" + featureName1 + "_parquet";
                if (algName.contains("topical"))
                    ffName = "/top"+topFeatureNum+"_" + featureName1;
                topFeatures = sqlContext.createDataFrame(sqlContext.read().parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/" + algName + ffName).javaRDD(),
                        new StructType(fieldsHashtag)).coalesce(numPart);
                df2 = df1;
                //HASHTAG , PROB, TID
                df2 = sqlContext.createDataFrame(topFeatures.join(df2, df2.col("hashtag").equalTo(topFeatures.col(fName))).drop(df2.col("hashtag")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                    @Override
                    public Tuple2<Long, Double> call(Row row) throws Exception {
                        return new Tuple2<Long, Double>(row.getLong(2), row.getDouble(1));
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                }).map(new Function<Tuple2<Long, Double>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Double> stringDoubleTuple2) throws Exception {
                        return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                    }
                }), new StructType(fieldsMap));
                df3 = df2.join(tweetTestTopical, df2.col("tid").equalTo(tweetTestTopical.col("tid"))).drop(tweetTestTopical.col("tid")).coalesce(numPart);
                df3 = df3.sort(df3.col("prob").desc()).limit(returnNum);
                df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_noTrainTweet_" + featureName1);
                df2 = df2.sort(df2.col("prob").desc()).limit(returnNum);
                df2 = df2.join(tweetTopical, df2.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
                //df2 = df2.join(df1, df1.col("tid").equalTo(df1.col("tid"))).drop(df1.col("tid")).coalesce(numPart);
                df2.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_TestTrain_" + featureName1);
                df3 = null;
            }
            System.out.println("=====================HASHTAG DONE======================");
        }
        if (calcMention) {
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_mentionFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
            System.out.println("============================ MENTION FEATURE IN TEST SET: " + df1.count() + "===============================================");
            //algNames = new String[]{"topical", "topicalLog", "MILog", "CP", "CPLog", "MI"};
            algNames = new String[]{"MI"};
            for (String algName : algNames) {
                final String featureName1 = "Mention";
                System.out.println("====================================== " + algName + " - " + featureName1 + "=======================================");
                String fName = "mentionee";
                String ffName = "/top"+topFeatureNum+"_" + featureName1 + "_parquet";
                if (algName.contains("topical"))
                    ffName = "/top"+topFeatureNum+"_" + featureName1;
                topFeatures = sqlContext.createDataFrame(sqlContext.read().parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/" + algName + ffName).javaRDD(),
                        new StructType(fieldsMention)).coalesce(numPart);
                df2 = df1;
                df2 = sqlContext.createDataFrame(topFeatures.join(df2, df2.col("mentionee").equalTo(topFeatures.col(fName))).drop(df2.col("mentionee")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                    @Override
                    public Tuple2<Long, Double> call(Row row) throws Exception {
                        return new Tuple2<Long, Double>(row.getLong(2), row.getDouble(1));
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                }).map(new Function<Tuple2<Long, Double>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Double> stringDoubleTuple2) throws Exception {
                        return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                    }
                }), new StructType(fieldsMap));
                df3 = df2.join(tweetTestTopical, df2.col("tid").equalTo(tweetTestTopical.col("tid"))).drop(tweetTestTopical.col("tid")).coalesce(numPart);
                df3 = df3.sort(df3.col("prob").desc()).limit(returnNum);
                df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_noTrainTweet_" + featureName1);
                df2 = df2.sort(df2.col("prob").desc()).limit(returnNum);
                df2 = df2.join(tweetTopical, df2.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
                df2.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_TestTrain_" + featureName1);
            }
            System.out.println("=====================Mention DONE======================");
        }
        if(calcLocation){

            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_locationFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
            //algNames = new String[]{"topical", "topicalLog", "MILog", "CP", "CPLog", "MI"};
            algNames = new String[]{"MI"};
            for (String algName : algNames) {
                final String featureName1 = "Location";
                System.out.println("====================================== " + algName + " - " + featureName1 + "=======================================");
                String fName = "location";
                String ffName = "/top"+topFeatureNum+"_" + featureName1 + "_parquet";
                if (algName.contains("topical"))
                    ffName = "/top"+topFeatureNum+"_" + featureName1;
                topFeatures = sqlContext.createDataFrame(sqlContext.read().parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/" + algName + ffName).javaRDD(),
                        new StructType(fieldsLocation)).coalesce(numPart);
                df2 = df1;
                df2 = sqlContext.createDataFrame(topFeatures.join(df2, df2.col("C1").equalTo(topFeatures.col(fName))).drop(df2.col("C1")).coalesce(numPart).javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                    @Override
                    public Tuple2<Long, Double> call(Row row) throws Exception {
                        return new Tuple2<Long, Double>(row.getLong(2), row.getDouble(1));
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                }).map(new Function<Tuple2<Long, Double>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Double> stringDoubleTuple2) throws Exception {
                        return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                    }
                }), new StructType(fieldsMap));
                df3 = df2.join(tweetTestTopical, df2.col("tid").equalTo(tweetTestTopical.col("tid"))).drop(tweetTestTopical.col("tid")).coalesce(numPart);
                df3 = df3.sort(df3.col("prob").desc()).limit(returnNum);
                df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_noTrainTweet_" + featureName1);
                df2 = df2.sort(df2.col("prob").desc()).limit(returnNum);
                df2 = df2.join(tweetTopical, df2.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
                df2.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_TestTrain_" + featureName1);
            }
            System.out.println("=====================LOCATION DONE======================");
        }

        if(calcTerm) {//ONLY MI FOR GNUM = 6
            //algNames = new String[]{ "topicalLog", "MILog", "CP","CPLog", "MI", "topical"};
            algNames = new String[]{"MI"};
            df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_termFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).coalesce(numPart);
            //df1.cache();
            for (String algName : algNames) {
                final String featureName1 = "Term";
                System.out.println("====================================== " + algName + " - " + featureName1 + "=======================================");
                String fName = "term";
                String ffName = "/top"+topFeatureNum+"_" + featureName1 + "_parquet";
                if (algName.contains("topical"))
                    ffName = "/top"+topFeatureNum+"_" + featureName1;
                topFeatures = sqlContext.createDataFrame(sqlContext.read().parquet(outputPath + "Baselines/" + groupNames[groupNum - 1] + "/" + algName + ffName).javaRDD(),
                        new StructType(fieldsTerm)).coalesce(numPart);
                df2 = df1.drop(tweetTime.col("time"));
                df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
                df2 = topFeatures.join(df2, df2.col("term").equalTo(topFeatures.col(fName))).drop(df2.col("term")).drop(topFeatures.col(fName)).drop(df2.col("time")).coalesce(numPart);
                df2 = sqlContext.createDataFrame(df2.javaRDD().mapToPair(new PairFunction<Row, Long, Double>() {
                    @Override
                    public Tuple2<Long, Double> call(Row row) throws Exception {
                        System.out.println(row.toString());
                        return new Tuple2<Long, Double>(row.getLong(1), row.getDouble(0));
                    }
                }).reduceByKey(new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double aDouble, Double aDouble2) throws Exception {
                        return aDouble + aDouble2;
                    }
                }).map(new Function<Tuple2<Long, Double>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Double> stringDoubleTuple2) throws Exception {
                        return RowFactory.create(stringDoubleTuple2._1(), stringDoubleTuple2._2());
                    }
                }), new StructType(fieldsMap));
                long c = df2.count();
                System.out.println("======================== TERM " + c);
                df2.write().parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_tmp");
                df2.persist(StorageLevel.MEMORY_AND_DISK_SER());

                df3 = df2.sort(df2.col("prob").desc()).limit(returnNum);
                df3 = df3.join(tweetTopical, df3.col("tid").equalTo(tweetTopical.col("tid"))).drop(tweetTopical.col("tid")).coalesce(numPart);
                df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_TestTrain_" + featureName1);

                df3 = df2.sort(df2.col("prob").desc()).limit((int)c/200);
                df3 = df3.join(tweetTestTopical, df3.col("tid").equalTo(tweetTestTopical.col("tid"))).drop(tweetTestTopical.col("tid")).coalesce(numPart);
                System.out.println(" NOTICE : " + c/200 + " "  + df3.count());
                df3 = df3.sort(df3.col("prob").desc()).limit(returnNum);
                df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/" + groupNames[groupNum - 1] + "/" + algName + "/qrel_top_noTrainTweet_" + featureName1);
            }
            System.out.println("=====================TERM DONE======================");
        }
    }

    public static void getTestTrainDataSet(SQLContext sqlContext) {
        String name = "tweet_hashtag_user_mention_term_time_parquet";
        if(allInnerJoin)
            name = "tweet_hashtag_user_mention_term_time_allInnerJoins_parquet";
        DataFrame df1 = sqlContext.read().parquet(outputPath + name).coalesce(numPart);
        DataFrame df2 = sqlContext.read().parquet(outputPath + "tweet_topical_chosenFeatureTweets_"+groupNum+"_parquet").coalesce(numPart);
        df2.registerTempTable("tweet_topical");
        df2 = df2.join(df1, df2.col("tid").equalTo(df1.col("tid"))).drop(df2.col("tid")).drop(df1.col("tid")).coalesce(numPart);
        df2.printSchema();
        //df2.cache();
        System.out.println("Count: " + df2.count());
        //df2.coalesce(numPart).write().format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save(outputPath + "testTrainData_" + groupNum + "_CSV");

        //System.out.println("================== COUNT: =========== " + df2.count());
        tweetUtil.output(df2.coalesce(1), "testTrainData_" + groupNum, false, outputPath);
        System.out.println("================== COUNT TOPICAL : " + sqlContext.sql("select count(*) from tweet_topical where topical = 1").head().get(0).toString() + "==============================");
    }

    private static void getTestTrainData(SQLContext sqlContext) {
        //Label Hashtag From Mention Term
        DataFrame positiveSamples, negativeSamples, df1, df2;
        String folderName = "baselineFeatures1000/";
        if (!thousand)
            folderName = "baselineFeatures5000/";
        folderName = "";
        DataFrame tweetTopical = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet").coalesce(numPart);
        if (groupNum > 1){

            //df2 = sqlContext.read().parquet(outputPath + "tweet_hashtag_user_mention_term_time_parquet").drop("user").drop("hashtag").drop("term").drop("mentionee").drop("time").drop("username").coalesce(numPart);//.registerTempTable(
            //System.out.println("=========== tweet_hashtag_user_mention_term_time COUNT =================== " + df2.count());
            //df = df.join(df2, df2.col("tid").equalTo(df.col("tid"))).drop(df2.col("tid")).coalesce(numPart);
            //negativeSamples = df.filter(df.col("topical").$eq$eq$eq(0)).coalesce(numPart);
            //positiveSamples = df.filter(df.col("topical").$greater(0)).coalesce(numPart);

            positiveSamples = tweetTopical.filter(tweetTopical.col("topical").$eq$eq$eq(1)).coalesce(numPart);
            negativeSamples = tweetTopical.filter(tweetTopical.col("topical").$eq$eq$eq(0)).coalesce(numPart);

            long l = positiveSamples.count();
            long l2 = negativeSamples.count();
            System.out.println("=================== POSITIVES/NEGATIVES LEFT ================ " + l + "/" + l2);
            double countVal = sampleNum - l;
            double sampleSize = (double) (countVal / l2);
            System.out.println("LOOOK: " + l + " " + l2);




            DataFrame featureTweetIds = sqlContext.read().parquet(outputPath + folderName + "tweet_thsh_fromFeature_grouped_parquet").drop("user")
                    .coalesce(numPart).unionAll(sqlContext.read().parquet(outputPath + folderName + "tweet_thsh_termFeature_grouped_parquet")
                            .drop("term").coalesce(numPart)).unionAll(sqlContext.read().
                            parquet(outputPath + folderName + "tweet_thsh_mentionFeature_grouped_parquet").drop("mentionee").coalesce(numPart))
                    .unionAll(sqlContext.read().parquet(outputPath + folderName + "tweet_thsh_hashtagFeature_grouped_parquet").drop("hashtag")
                            .coalesce(numPart)).unionAll(sqlContext.read().parquet(outputPath + folderName + "tweet_thsh_locationFeature_grouped_parquet")
                            .drop("location").coalesce(numPart)).coalesce(numPart).distinct();
            //featureTweetIds.write().parquet(outputPath + "featureTweetIds_parquet");
            long featureTweetIdsCount = featureTweetIds.count();
            System.out.println("================== featureTweetIds COUNT: =========== " + featureTweetIdsCount);
            DataFrame negativeTweetIds = featureTweetIds.sample(false, sampleSize).coalesce(numPart);

            long c = negativeTweetIds.count();
            System.out.println("================== negativeTweetIds COUNT: =========== " + c);
            while (c < sampleNum - l - featureNumWin) {
                featureTweetIds = featureTweetIds.except(negativeTweetIds);
                long tmpCount = featureTweetIdsCount - c;//featureTweetIds.count();
                System.out.println("================== featureTweetIds COUNT 2: =========== " + tmpCount);
                sampleSize = (double) (sampleNum - l - c) / tmpCount;
                System.out.println("==================SAMPLE SIZE: ============" + sampleSize);
                negativeTweetIds = negativeTweetIds.unionAll(featureTweetIds.sample(false, sampleSize).coalesce(numPart));
                c = negativeTweetIds.count();
                System.out.println("================== negativeTweetIds COUNT2: =========== " + c);
            }

            featureTweetIds = negativeTweetIds.unionAll(positiveSamples.select("tid")).coalesce(numPart).distinct();
            //System.out.println("================== positiveTweetIds COUNT2: =========== " + positiveSamples.count());
            System.out.printf("================POSITIVE AT BEGIN: " + featureTweetIds.join(tweetTopical, featureTweetIds.col("tid").equalTo(tweetTopical.col("tid"))).javaRDD().filter(new Function<Row, Boolean>() {
                @Override
                public Boolean call(Row v1) throws Exception {
                    return v1.get(2).toString().equals("1");
                }
            }).count() + "=================");
            featureTweetIds.write().mode(SaveMode.Overwrite).parquet(outputPath + "featureTweetIds_parquet");
            //System.out.println("================== featureTweetIds COUNT: =========== " + featureTweetIds.count());

            df2 = sqlContext.read().parquet(dataPath + folderName + "tweet_fromFeature_grouped_parquet").coalesce(numPart);
            //df1 = sqlContext.read().parquet(dataPath + "user_location_parquet").coalesce(numPart);
            //df2 = df2.join(df1, df2.col("user").equalTo(df1.col("username")), "left").drop(df1.col("username"));
            //df2.printSchema(); df2.show();
            df1 = featureTweetIds.join(df2, df2.col("tid").equalTo(featureTweetIds.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

            df2 = sqlContext.read().parquet(dataPath + folderName + "tweet_termFeature_grouped_parquet").coalesce(numPart);
            df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
            //df1.write().parquet(outputPath + "tweet_tmp1_parquet");

            //df1 = sqlContext.read().parquet(outputPath + "tweet_tmp1_parquet");
            //System.out.println("================== TMP1 COUNT: =========== " + df1.count());
            df2 = sqlContext.read().parquet(dataPath + folderName + "tweet_hashtagFeature_grouped_parquet").coalesce(numPart);
            df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
            df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_tmp2_parquet");
        }

        //System.out.println("================== TMP2 COUNT: =========== " + df1.count());
        df1 = sqlContext.read().parquet(outputPath + "tweet_tmp2_parquet").coalesce(numPart);
        df1.cache();
        df2 = sqlContext.read().parquet(dataPath + folderName + "tweet_mentionFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        //System.out.println("================== COUNT: =========== " + df1.count());
        //df1.cache();

        df2 = sqlContext.read().parquet(dataPath + folderName + "tweet_locationFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);//.registerTempTable("tweetMention");
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        df1 = df1.join(tweetTopical, tweetTopical.col("tid").equalTo(df1.col("tid"))).drop(df1.col("tid")).coalesce(numPart);
        tweetUtil.output(df1.coalesce(numPart), folderName+"tweet_hashtag_user_mention_term_time_location_" + groupNum + "_allInnerJoins", false, outputPath);

        System.out.printf("================POSITIVE AT END: " + df1.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row v1) throws Exception {
                return Integer.valueOf(v1.get(7).toString()) == 1;
            }
        }).count() + "=================");
        /*df1 = sqlContext.read().parquet(outputPath + "tweet_hashtag_user_mention_term_time_location_" + groupNum + "_allInnerJoins_parquet").coalesce(numPart);
        df2 = sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "FeaturesList_csv").coalesce(numPart);
        df1 = df1.join(df2, df1.col("user").equalTo(df2.col("C1")), "left").drop(df2.col("C1")).drop(df1.col("user")).coalesce(numPart);
        df1 = df1.select(df1.col("tid"), df1.apply("C0").as("user"), df1.col("term"), df1.col("hashtag"), df1.col("mentionee"), df1.col("location"), df1.col("time"));
        df1 = df1.join(df2, df1.col("location").equalTo(df2.col("C1")), "left").drop(df2.col("C1")).drop(df1.col("location")).coalesce(numPart);
        df1 = df1.select(df1.col("tid"), df1.col("user"), df1.col("term"), df1.col("hashtag"), df1.col("mentionee"), df1.apply("C0").as("location"), df1.col("time"));

        df2 = sqlContext.read().parquet(dataPath + "tweet_hashtag_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "left").drop(df1.col("hashtag")).drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(dataPath + "tweet_mention_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "left").drop(df1.col("mentionee")).drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(dataPath + "tweet_term_hashtag_grouped_parquet").drop("hashtag").coalesce(numPart);
        df1 = df1.join(df2, df1.col("tid").equalTo(df2.col("tid")), "left").drop(df1.col("term")).drop(df2.col("tid")).coalesce(numPart);

        tweetUtil.output(df1, "tweet_hashtag_user_mention_term_time_location_strings_"+groupNum+"_allInnerJoins", false);*/

        //System.out.println("================== Only tweets with chosen features TWEET TOPICAL COUNT: " + df1.count() + "========================");
        //System.out.println("================== Only tweets with chosen features TWEET TOPICAL POSITIVE COUNT: " + positiveSamples.count() + "========================");
        //System.out.println("================== Only tweets with chosen features TWEET TOPICAL NEGATIVE COUNT: " + negativeSamples.count() + "========================");


        //System.out.println("================== FINAL TWEET COUNT: =========== " + df1.count());

    }

    private static void getTestTrainDataMixedAllData(SQLContext sqlContext) throws ParseException {
        DataFrame df1, df2;
        df2 = sqlContext.read().parquet(dataPath + "tweet_time_parquet").coalesce(numPart);
        //Get all tweets after max(SplitTime), so we can seperate them later based on each topics' splitTime
        long maxTimeStamp = -1;
        for(long a : timestamps){
            if(a > maxTimeStamp)
                maxTimeStamp = a;
        }
        if(configRead.getTestFlag()){
            final SimpleDateFormat format = new SimpleDateFormat("EEE MMM dd HH':'mm':'ss zz yyyy");
            final long splitTime = format.parse("Thu Feb 20 15:08:01 +0001 2014").getTime();
            maxTimeStamp = splitTime;
            maxTimeStamp = 15000000000000000l;
        }


        df1 = df2.filter(df2.col("time").gt(maxTimeStamp));//tid, time for all tweets after max(splitTime)
        String folderName = "";


        df2 = sqlContext.read().parquet(outputPath + "tweet_hashtagFeature_grouped_parquet").drop("username").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "tweet_hashtag_time_" + groupNum + "_allInnerJoins");

        df2 = sqlContext.read().parquet(outputPath + folderName + "tweet_fromFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(outputPath + folderName + "tweet_locationFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        df2 = sqlContext.read().parquet(outputPath + folderName + "tweet_mentionFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);
        tweetUtil.output(df1.coalesce(numPart), folderName + "tweet_hashtag_user_mention_time_location_" + groupNum + "_allInnerJoins", false, outputPath);

        df1 = sqlContext.read().parquet(outputPath + folderName + "tweet_hashtag_user_mention_time_location_" + groupNum + "_allInnerJoins_parquet");
        df2 = sqlContext.read().parquet(outputPath + folderName + "tweet_termFeature_grouped_parquet").coalesce(numPart);
        df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        //df2 = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "_parquet").coalesce(numPart);
        //df1 = df1.join(df2, df2.col("tid").equalTo(df1.col("tid")), "left").drop(df2.col("tid")).coalesce(numPart);

        tweetUtil.output(df1.coalesce(numPart), folderName + "tweet_hashtag_user_mention_term_time_location_" + groupNum + "_allInnerJoins_allTrainData_AfterMaxSplitTime_", false, outputPath);

    }
}
