package misc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class Query {
    protected static int numParts = 640;
    private static String dataPath = "data/";
    private static String featuresPath = "features/";
    private static String outputPath = "output/";

    public static void main(String args[]) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Query");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numParts);

        // base
        sqlContext.read().parquet(dataPath + "tid_uid_ts.parquet").registerTempTable("tweets");
        sqlContext.read().parquet(dataPath + "tid_hid.parquet").registerTempTable("tid_hid");
        sqlContext.read().parquet(dataPath + "tid_mid.parquet").registerTempTable("tid_mid");
        sqlContext.read().parquet(dataPath + "uid_uname.parquet").registerTempTable("users");
        sqlContext.read().parquet(dataPath + "hid_hashtag.parquet").registerTempTable("hashtags");
        sqlContext.cacheTable("tweets");
        sqlContext.cacheTable("tid_hid");
        sqlContext.cacheTable("tid_mid");
        sqlContext.cacheTable("users");
        sqlContext.cacheTable("hashtags");

        // features
        sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
            .load(featuresPath + "from.csv").registerTempTable("featuresFrom");
        sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
            .load(featuresPath + "mention.csv").registerTempTable("featuresMention");
        sqlContext.read().format("com.databricks.spark.csv").option("header", "true")
            .load(featuresPath + "hashtag.csv").registerTempTable("featuresHashtag");
        //sqlContext.cacheTable("featuresFrom");
        //sqlContext.cacheTable("featuresMention");
        //sqlContext.cacheTable("featuresHashtag");

        for (int i = 0; i < 3; ++i) {
            DataFrame from = sqlContext.sql(
                "SELECT t.tid" +
                    " FROM featuresFrom AS ff, users AS u, tweets AS t" +
                    " WHERE u.uname = ff.uname AND u.uid = t.uid"
            );
            DataFrame mention = sqlContext.sql(
                "SELECT tm.tid" +
                    " FROM featuresMention AS fm, users AS u, tid_mid AS tm" +
                    " WHERE u.uname = fm.uname AND u.uid = tm.mid"
            );
            DataFrame hashtag = sqlContext.sql(
                "SELECT th.tid" +
                    " FROM featuresHashtag AS fh, hashtags AS h, tid_hid AS th" +
                    " WHERE h.hashtag = fh.hashtag AND h.hid = th.hid"
            );
            DataFrame tweets = from.unionAll(mention).unionAll(hashtag).distinct();
            tweets.write().mode(SaveMode.Overwrite).parquet(outputPath + "resultTweets.parquet");
        }
    }
}









