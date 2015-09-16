package lagtime;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Random;

public class LagTime implements Serializable {
    protected static int numParts = 32 * 3;
    protected static StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();
    protected static long cgSize = 1000;
    private static String dataPath = "data/";
    private static String outputPath = "output/";

    private static Random randObj = new Random();

    public static void main(String args[]) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("LagTime");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numParts);

        // read data
        DataFrame tweets = sqlContext.read().parquet(dataPath + "tid_uid_ts.parquet");
        DataFrame uid_mid = sqlContext.read().parquet(dataPath + "uid_mid.parquet");
        DataFrame users = sqlContext.read().parquet(dataPath + "usersWhoMention.parquet");
        DataFrame hashtagBirthdaysPerUser = sqlContext.read().parquet(dataPath + "hashtagBirthdaysPerUser.parquet");
        DataFrame hashtagTimestamps = sqlContext.read().parquet(dataPath + "hashtagTimestamps.parquet");

        tweets = tweets.drop("ts").persist(storageLevel);
        uid_mid = uid_mid.persist(storageLevel);
        users = users.persist(storageLevel);
        hashtagBirthdaysPerUser = hashtagBirthdaysPerUser.persist(storageLevel);
        hashtagTimestamps = hashtagTimestamps.persist(storageLevel);

        tweets.registerTempTable("tweets");
        uid_mid.registerTempTable("uid_mid");
        users.registerTempTable("users");
        hashtagBirthdaysPerUser.registerTempTable("hashtagBirthdaysPerUser");
        hashtagTimestamps.registerTempTable("hashtagTimestamps");

        uid_mid = null;
        hashtagBirthdaysPerUser = null;
        hashtagTimestamps = null;
        long tweetCount = 829026458; //known
        long userCount = users.count();

        DataFrame[] hbGroups = {
            sqlContext.read().parquet(dataPath + "generalHashtagBirthdays.parquet"),
            sqlContext.read().parquet(dataPath + "disasterHashtagBirthdays.parquet"),
            sqlContext.read().parquet(dataPath + "politicHashtagBirthdays.parquet"),
            sqlContext.read().parquet(dataPath + "socialHashtagBirthdays.parquet")
        };
        DataFrame[] ltuTweetGroups = {
            sqlContext.read().parquet(dataPath + "general1000LTUTweets.parquet"),
            sqlContext.read().parquet(dataPath + "disaster1000LTUTweets.parquet"),
            sqlContext.read().parquet(dataPath + "politic1000LTUTweets.parquet"),
            sqlContext.read().parquet(dataPath + "social1000LTUTweets.parquet")
        };
        String[] names = {"general", "disaster", "politic", "social"};

        int numIter = 20;
        DataFrame[] lagTimes = new DataFrame[6];

        for (int i = 0; i < hbGroups.length; ++i) {
            ltuTweetGroups[i] = ltuTweetGroups[i].persist(storageLevel);
            hbGroups[i] = hbGroups[i].persist(storageLevel);
            hbGroups[i].registerTempTable("hbG");
            long ltuTCount = ltuTweetGroups[i].count();

            //not every user uses all the hashtags
            sqlContext.sql(
                "SELECT hbpu.uid, hbpu.hid, hbpu.birthday" +
                    " FROM hbG, hashtagBirthdaysPerUser AS hbpu" +
                    " WHERE hbG.hid = hbpu.hid"
            ).registerTempTable("tmpHashtagBirthdaysPerUser");
            sqlContext.sql(
                "SELECT hT.tid, hT.hid, hT.ts" +
                    " FROM hbG, hashtagTimestamps AS hT" +
                    " WHERE hbG.hid = hT.hid"
            ).registerTempTable("tmpHashtagTimestamps");
            sqlContext.cacheTable("tmpHashtagBirthdaysPerUser");
            sqlContext.cacheTable("tmpHashtagTimestamps");

            /************* do work ***********/

            for (int j = 0; j < numIter; ++j) {
                System.out.println("===== ITERATION " + i + "-" + j + " =====");

                String CG_uid = getUsersAsString(
                    users.sample(false, ((double) cgSize) / userCount * 1.1).limit((int) cgSize)
                );

                //****** there's got to be a better way to do this ******
                JavaRDD<Long> tmpSG = sqlContext.sql(
                    "SELECT uid, mid FROM uid_mid WHERE uid IN " + CG_uid
                ).javaRDD().mapToPair(
                    new PairFunction<Row, Long, String>() {
                        @Override
                        public Tuple2<Long, String> call(Row row) throws Exception {
                            return new Tuple2<>(row.getLong(0), Long.toString(row.getLong(1), 10));
                        }
                    }
                ).reduceByKey(
                    new Function2<String, String, String>() {
                        @Override
                        public String call(String s, String s2) throws Exception {
                            return s + " , " + s2;
                        }
                    }
                ).map(
                    new Function<Tuple2<Long, String>, Long>() {
                        @Override
                        public Long call(Tuple2<Long, String> tuple) throws Exception {
                            String[] ml = tuple._2().split(" , ");
                            return Long.parseLong(ml[randObj.nextInt(ml.length)], 10);
                        }
                    }
                );

                String SG_uid = getUsersAsString(tmpSG);
                //********************************************************

                long M = sqlContext.sql(
                    "SELECT * FROM tweets WHERE uid IN " + CG_uid
                ).count();

                DataFrame B = sqlContext.sql(
                    "SELECT tid FROM tweets WHERE uid IN " + SG_uid
                );
                B.persist(storageLevel);
                long N = B.count();

                DataFrame C = tweets.sample(false, ((double) N) / tweetCount * 1.1).limit((int) N).drop("uid");
                DataFrame D = B.sample(false, ((double) M) / N * 1.1).limit((int) M);
                DataFrame E = ltuTweetGroups[i].sample(false, ((double) M) / ltuTCount * 1.1).limit((int) M);
                DataFrame F = ltuTweetGroups[i].sample(false, ((double) N) / ltuTCount * 1.1).limit((int) N);

                lagTimes[0] = getLagTime(sqlContext, CG_uid);
                lagTimes[1] = getLagTime(sqlContext, SG_uid);
                lagTimes[2] = getLagTime(C);
                lagTimes[3] = getLagTime(D);
                lagTimes[4] = getLagTime(E);
                lagTimes[5] = getLagTime(F);

                saveResult(lagTimes, outputPath + names[i] + "Result.parquet");

                B.unpersist();
            }

            /******************* done ******************/

            sqlContext.uncacheTable("tmpHashtagBirthdaysPerUser");
            sqlContext.uncacheTable("tmpHashtagTimestamps");
            ltuTweetGroups[i].unpersist();
            hbGroups[i].unpersist();
            sqlContext.dropTempTable("hbG");
            sqlContext.dropTempTable("tmpHashtagBirthdaysPerUser");
            sqlContext.dropTempTable("tmpHashtagTimestamps");
        }
    }

    private static String getUsersAsString(DataFrame users) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (Row r: users.collect()) { //might cause OOM on driver !!!
            sb.append(r.getLong(0)).append(',');
        }
        int lastIdx = sb.length() - 1;
        if (sb.charAt(lastIdx) == ',') {
            sb.deleteCharAt(lastIdx);
        }
        sb.append(')');

        return sb.toString();
    }

    private static String getUsersAsString(JavaRDD<Long> users) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (long i: users.collect()) { //might cause OOM on driver !!!
            sb.append(i).append(',');
        }
        int lastIdx = sb.length() - 1;
        if (sb.charAt(lastIdx) == ',') {
            sb.deleteCharAt(lastIdx);
        }
        sb.append(')');

        return sb.toString();
    }

    private static DataFrame getLagTime(SQLContext sqlContext, String vals) {
        return doGetLagTime(sqlContext.sql(
            "SELECT hid, min(birthday) AS time" +
                " FROM tmpHashtagBirthdaysPerUser" +
                " WHERE uid IN " + vals +
                " GROUP BY hid"
        ));
    }

    private static DataFrame getLagTime(DataFrame T) {
        SQLContext sqlContext = T.sqlContext();
        T.registerTempTable("T");
        DataFrame tmp = sqlContext.sql(
            "SELECT hT.hid, min(hT.ts) AS time" +
                " FROM T, tmpHashtagTimestamps AS hT" +
                " WHERE T.tid = hT.tid" +
                " GROUP BY hT.hid"
        );
        sqlContext.dropTempTable("T");
        return doGetLagTime(tmp);
    }

    private static DataFrame doGetLagTime(DataFrame lagTimes) {
        SQLContext sqlContext = lagTimes.sqlContext();
        lagTimes.registerTempTable("tmp");
        DataFrame tmp = sqlContext.sql(
            "SELECT hbG.hid, coalesce(tmp.time - hbG.birthday, 60393600) AS lagTime" + // upper limit
                " FROM hbG" +
                " LEFT OUTER JOIN tmp ON hbG.hid = tmp.hid"
        );
        sqlContext.dropTempTable("tmp");
        return tmp;
    }

    private static void saveResult(DataFrame[] lagTimes, String folderPath) { // must have >= 1 groups
        int size = lagTimes.length;
        assert (size < 1); // can't happen

        StringBuilder sb = new StringBuilder();
        SQLContext sqlContext = lagTimes[0].sqlContext();
        for (int i = 0; i < size; ++i) {
            lagTimes[i].registerTempTable("lt" + i);
        }

        sb.append("SELECT DISTINCT lt0.hid ");
        for (int i = 0; i < size; ++i) {
            sb.append(",lt").append(i).append(".lagTime ").append("AS lagTime").append(i).append(" ");
        }
        sb.append("FROM lt0 ");
        for (int i = 1; i < size; ++i) {
            sb.append("INNER JOIN lt").append(i)
                .append(" ON lt").append(i-1).append(".hid = lt").append(i).append(".hid ");
        }

        sqlContext.sql(sb.toString()).coalesce(numParts).write().mode(SaveMode.Append).parquet(folderPath);
        for (int i = 0; i < size; ++i) {
            sqlContext.dropTempTable("lt" + i);
        }
    }
}














