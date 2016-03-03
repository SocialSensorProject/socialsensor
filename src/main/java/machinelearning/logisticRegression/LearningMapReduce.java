package machinelearning.logisticRegression;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import util.ConfigRead;
import scala.Tuple2;
import util.TweetUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by imanz on 11/25/15.
 */
public class LearningMapReduce {

    private static String hdfsPath;
    private static String dataPath; //"TestSet/";
    private static String outputPath; // "TestSet/output_all/";
    private static ConfigRead configRead;
    private static int groupNum = 1;
    private static final double userCountThreshold = 10;
    private static long featureNum = 1000000;
    private static long sampleNum = 2000000;
    private static final double featureNumWin = 1000;
    private static final boolean allInnerJoin = false;
    private static boolean localRun;
    private static int numOfGroups;
    private static String[] groupNames;
    private static int returnNum = 10000;
    private static int numPart;
    private static final long[] timestamps= {1377897403000l, 1362146018000l, 1391295058000l, 1372004539000l, 1359920993000l, 1364938764000l, 1378911100000l, 1360622109000l, 1372080004000l, 1360106035000l};;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String[] args) throws IOException, ParseException {
        groupNames = configRead.getGroupNames();
        numPart = configRead.getNumPart();
        hdfsPath = configRead.getHdfsPath();
        dataPath = hdfsPath + configRead.getDataPath(); //configRead.getTestDataPath();
        outputPath = hdfsPath + configRead.getOutputPath(); //configRead.getLocalOutputPath()
        localRun = configRead.isLocal();
        SparkConf sparkConfig;
        if(localRun) {
            numPart = 4;
            featureNum = 20;
            sampleNum = 50;
            dataPath = configRead.getTestDataPath();
            dataPath = configRead.getTestDataPath();
            outputPath = configRead.getTestOutPath();
            sparkConfig = new SparkConf().setAppName("SparkTest").setMaster("local[2]");
        }else {
            sparkConfig = new SparkConf().setAppName("SparkTest");
        }
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);

        getLearningBaseline(sqlContext);
    }

    private static void getLearningBaseline(SQLContext sqlContext) throws ParseException, IOException {

        StructField[] fieldsFrom = {
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("prob", DataTypes.DoubleType, true)
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
        String[] algNames = { "CP", "CPLog","MI", "MILog", "topical", "topicalLog"};

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

        FileReader fileReaderA; BufferedReader bufferedReaderA;
        boolean skipFeature = false;



        String featureName = "featureWeights_from";
        //dataPath = "/data/ClusterData/Output/BaselinesRes/Mixed/MI/";
        System.out.printf("********************* " + dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");


        fileReaderA = new FileReader(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        skipFeature = (bufferedReaderA.readLine() == null);
        if(!skipFeature) {
            //df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_fromFeature_grouped_parquet").coalesce(numPart);
            //df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            //df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = sqlContext.read().parquet(outputPath + "from_time_" + groupNum + "_parquet");;
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
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
            df3 = topFeatures.join(df2, df2.col("username").equalTo(topFeatures.col("username"))).drop(topFeatures.col("username")).drop("username").coalesce(numPart);

            System.out.println("*****************************FROM : " + df3.count() + "**************************");
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile0");
            System.out.println("\nAFTER WRITING FROM");
            df2.unpersist();
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile0");
        }
        bufferedReaderA.close();

        featureName = "featureWeights_hashtag";
        fileReaderA = new FileReader(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        skipFeature = (bufferedReaderA.readLine() == null);
        if(!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
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
            /*df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_hashtagFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);*/
            df2 = sqlContext.read().parquet(outputPath + "hashtag_time_" + groupNum + "_parquet");;
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
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
            }else
                df3 = df1;
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile1");
            System.out.println("*****************************HASHTAG : " + df3.count() + "**************************");
            //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob) AS weight1 FROM fromFeature m1,hashtagFeature m2 on m1.tid = m2.tid").coalesce(numPart);
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum-1] + "/top1000Tweets1.csv");

            //hashtagFeat = hashtagFeat.sort(hashtagFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(hashtagFeat, fromFeat.col("tid").equalTo(hashtagFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile1");
        }
        bufferedReaderA.close();
        System.out.println("\n HASHTAG DONE \n");


        featureName = "featureWeights_mention";
        fileReaderA = new FileReader(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        skipFeature = (bufferedReaderA.readLine() == null);
        if(!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
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
            /*df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_mentionFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;*/
            df2 = sqlContext.read().parquet(outputPath + "Mention_time_" + groupNum + "_parquet");;
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
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
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile2");
            System.out.println("*****************************MENTION : " + df3.count() + "**************************");
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv");
            //mentionFeat = mentionFeat.sort(mentionFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(mentionFeat, mentionFeat.col("tid").equalTo(mentionFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile2");
        }
        bufferedReaderA.close();


        featureName = "featureWeights_term";
        fileReaderA = new FileReader(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        skipFeature = (bufferedReaderA.readLine() == null);
        if(!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
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
            /*df2 = sqlContext.read().parquet(dataPath + "tweet_thsh_termFeature_grouped_parquet").coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;*/
            df2 = sqlContext.read().parquet(outputPath + "Term_time_" + groupNum + "_parquet");;
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
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
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile");
            //System.out.println("*****************************TERM : " + df1.count() + "**************************");
            //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.prob) AS weight2 FROM mentionFeature m1,termFeature m2").coalesce(numPart);
            //df3.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets2.csv");
            //termFeat = termFeat.sort(termFeat.col("prob").desc()).limit(returnNum);
            //fromFeat = fromFeat.join(termFeat, termFeat.col("tid").equalTo(termFeat.col("tid")), "outer").drop(fromFeat.col("tid")).coalesce(numPart);
            df3 =  sqlContext.read().parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile");
        }
        bufferedReaderA.close();

        //sqlContext.read().parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets1.csv").registerTempTable("table1");
        featureName = "featureWeights_location";
        fileReaderA = new FileReader(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv");
        bufferedReaderA = new BufferedReader(fileReaderA);
        skipFeature = (bufferedReaderA.readLine() == null);
        if (!skipFeature) {
            topFeatures = sqlContext.createDataFrame(sqlContext.read().format("com.databricks.spark.csv").load(dataPath + "Data/Learning/Topics/" + groupNames[groupNum - 1] + "/fold0/l2_lr/" + featureName + ".csv").drop("C0").javaRDD().map(new Function<Row, Row>() {
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
            /*df2 = sqlContext.createDataFrame(sqlContext.read().parquet(dataPath + "tweet_thsh_locationFeature_grouped_parquet").javaRDD(), new StructType(fieldsLocation)).coalesce(numPart);
            df1 = df2.join(tweetTime, df2.col("tid").equalTo(tweetTime.col("tid"))).drop(tweetTime.col("tid")).drop("time").coalesce(numPart);
            df2 = df1;*/
            df2 = sqlContext.read().parquet(outputPath + "Location_time_" + groupNum + "_parquet");
            df2.printSchema();
            df2.persist(StorageLevel.MEMORY_AND_DISK_SER());
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
            df3.write().mode(SaveMode.Overwrite).parquet(outputPath+"LearningTweetWeights_" + groupNames[groupNum - 1] + "_tmpFile");
            //System.out.println("*****************************LOCATION : " + df1.count() + "**************************");
            //df1 = sqlContext.sql("SELECT m1.tid, (m1.prob+m2.weight1) AS weight3 FROM locationFeature m1,table1 m2").coalesce(numPart);
        }
        bufferedReaderA.close();
        df2 = sqlContext.read().parquet(outputPath + "tweet_topical_" + groupNum + "negative_train_parquet").coalesce(numPart);
        df1 = df3.join(df2, df3.col("tid").equalTo(df2.col("tid"))).drop(df2.col("tid"));
        df1 = df1.sort(df3.col("prob").desc()).limit(returnNum);
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets_noTrainTweet.csv");

        df3 = df3.join(tweetTopical, tweetTopical.col("tid").equalTo(df3.col("tid")), "left").drop(tweetTopical.col("tid")).coalesce(numPart);
        df1 = df3.sort(df3.col("prob").desc()).limit(returnNum);
        df1.write().mode(SaveMode.Overwrite).parquet(outputPath + "BaselinesRes/Learning/Topics/" + groupNames[groupNum - 1] + "/top1000Tweets.csv");
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
}
