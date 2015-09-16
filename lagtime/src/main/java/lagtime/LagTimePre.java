package lagtime;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

import java.io.Serializable;

public class LagTimePre implements Serializable {
    private static String dataPath = "data/";
    private static String outputPath = "data/";
    private static int numParts = LagTime.numParts * 7;

    // NEED ~600 GB work/local dir
    public static void main(String args[]) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("LagTimePre");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numParts);

        // read data
        DataFrame tweets = sqlContext.read().parquet(dataPath + "tid_uid_ts.parquet");
        DataFrame tid_hid = sqlContext.read().parquet(dataPath + "tid_hid.parquet");
        DataFrame tid_mid = sqlContext.read().parquet(dataPath + "tid_mid.parquet");
        DataFrame hashtags = sqlContext.read().parquet(dataPath + "hid_hashtag.parquet");

        tweets = tweets.persist(LagTime.storageLevel);
        hashtags = hashtags.persist(LagTime.storageLevel);

        tweets.registerTempTable("tweets");
        hashtags.registerTempTable("hashtags");
        tid_hid.registerTempTable("tid_hid");
        tid_mid.registerTempTable("tid_mid");

        // uid_mid
        DataFrame uid_mid = sqlContext.sql(
            "SELECT DISTINCT t.uid, m.mid" +
                " FROM tweets AS t, tid_mid AS m" +
                " WHERE t.tid = m.tid"
        );
        sqlContext.dropTempTable("tid_mid");
        uid_mid = uid_mid.persist(LagTime.storageLevel);
        uid_mid.registerTempTable("uid_mid");
        uid_mid.write().parquet(outputPath + "uid_mid.parquet");

        // distinct uid (within uid_mid)
        sqlContext.sql(
            "SELECT DISTINCT uid" +
                " FROM uid_mid"
        ).write().parquet(outputPath + "usersWhoMention.parquet");
        sqlContext.dropTempTable("uid_mid");
        uid_mid.unpersist();

        // hashtag timestamps
        DataFrame hTs = sqlContext.sql(
            "SELECT t.tid, t.uid, h.hid, t.ts" +
                " FROM tweets AS t, tid_hid AS h" +
                " WHERE t.tid = h.tid"
        );
        sqlContext.dropTempTable("tid_hid");
        hTs = hTs.persist(LagTime.storageLevel);
        hTs.registerTempTable("hashtagTimestamps");
        hTs.write().parquet(outputPath + "hashtagTimestamps.parquet");

        // hashtag birthdays
        sqlContext.sql(
            "SELECT hid, min(ts) AS birthday" +
                " FROM hashtagTimestamps" +
                " GROUP BY hid"
        ).registerTempTable("hashtagBirthdays");
        sqlContext.cacheTable("hashtagBirthdays");
        sqlContext.table("hashtagBirthdays").write().parquet(outputPath + "hashtagBirthdays.parquet");

        // hashtag birthdays per user
        sqlContext.sql(
            "SELECT uid, hid, min(ts) AS birthday" +
                " FROM hashtagTimestamps" +
                " GROUP BY uid, hid"
        ).registerTempTable("hashtagBirthdaysPerUser");
        sqlContext.cacheTable("hashtagBirthdaysPerUser");
        sqlContext.table("hashtagBirthdaysPerUser").write().parquet(outputPath + "hashtagBirthdaysPerUser.parquet");


        /******** categorized hashtags & respective lead-time users *********/

        // general
        DataFrame general = sqlContext.sql(
            "SELECT hid, min(ts) AS birthday" +
                " FROM hashtagTimestamps" +
                " GROUP BY hid" +
                " HAVING ((count(hid) BETWEEN 2500 AND 500000) AND (min(ts) > 1362095999))" // 02/28/2013 23:59:59
        );
        getLTU(general, "general");
        hTs.unpersist();
        sqlContext.dropTempTable("hashtagTimestamps");

        // the rest
        DataFrame[] hG = {
            // disaster
            sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "true")
                .load(dataPath + "disasterHashtags.csv"),
            // politic
            sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "true")
                .load(dataPath + "politicHashtags.csv"),
            // social
            sqlContext.read().format("com.databricks.spark.csv")
                .option("header", "true")
                .load(dataPath + "socialHashtags.csv")
        };
        String[] names = {"disaster", "politic", "social"};

        getHBnLTU(hG, names);
    }

    private static void getHBnLTU(DataFrame[] hG, String[] names) {
        for (int i = 0; i < hG.length; ++i) {
            SQLContext sqlContext = hG[i].sqlContext();

            hG[i].registerTempTable("tmp");
            DataFrame hb = sqlContext.sql(
                "SELECT hb.hid, hb.birthday" +
                    " FROM tmp, hashtags AS h, hashtagBirthdays AS hb" +
                    " WHERE tmp.hashtag = h.hashtag AND h.hid = hb.hid"
            );
            sqlContext.dropTempTable("tmp");
            getLTU(hb, names[i]);
        }
    }

    private static void getLTU(DataFrame hb, String name) {
        SQLContext sqlContext = hb.sqlContext();

        hb.registerTempTable("tmpHashtagBirthdays");
        sqlContext.cacheTable("tmpHashtagBirthdays");
        sqlContext.table("tmpHashtagBirthdays").write()
            .parquet(outputPath + name + "HashtagBirthdays.parquet");

        sqlContext.sql(
            "SELECT hbpu.uid, hbpu.hid, hbpu.birthday" +
                " FROM tmpHashtagBirthdays AS hb, hashtagBirthdaysPerUser AS hbpu" +
                " WHERE hb.hid = hbpu.hid"
        ).registerTempTable("tmpHashtagBirthdaysPerUser");
        sqlContext.cacheTable("tmpHashtagBirthdaysPerUser");

        sqlContext.sql(
            "SELECT DISTINCT uid" +
                " FROM tmpHashtagBirthdaysPerUser AS hbpu"
        ).join(
            sqlContext.table("tmpHashtagBirthdays")
        ).coalesce(numParts * 10).registerTempTable("tmp");

        sqlContext.sql(
            "SELECT tmp.uid, avg(coalesce(hbpu.birthday - tmp.birthday, 60393600)) AS avglagTime" +
                " FROM tmp" +
                " LEFT OUTER JOIN tmpHashtagBirthdaysPerUser AS hbpu" +
                " ON (tmp.uid = hbpu.uid AND tmp.hid = hbpu.hid)" +
                " GROUP BY tmp.uid"
        ).coalesce(numParts).registerTempTable("leadTimeUsers");
        sqlContext.cacheTable("leadTimeUsers");
        sqlContext.dropTempTable("tmp");

        sqlContext.table("leadTimeUsers").write().parquet(outputPath + name + "LeadTimeUsers.parquet");
        sqlContext.sql(
            "SELECT t.tid" +
                " FROM tweets AS t," +
                " (SELECT uid FROM leadTimeUsers ORDER BY avglagTime LIMIT " + LagTime.cgSize + ") AS tmp" +
                " WHERE t.uid = tmp.uid"
        ).write().parquet(outputPath + name + LagTime.cgSize + "LTUTweets.parquet");

        sqlContext.uncacheTable("tmpHashtagBirthdays");
        sqlContext.uncacheTable("tmpHashtagBirthdaysPerUser");
        sqlContext.uncacheTable("leadTimeUsers");
        sqlContext.dropTempTable("tmpHashtagBirthdays");
        sqlContext.dropTempTable("tmpHashtagBirthdaysPerUser");
        sqlContext.dropTempTable("leadTimeUsers");
    }
}














