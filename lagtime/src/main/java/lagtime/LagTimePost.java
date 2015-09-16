package lagtime;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.Serializable;

public class LagTimePost implements Serializable {
    private static String dataPath = "data/";
    private static String outputPath = "output/";
    private static int numParts = LagTime.numParts;

    public static void main(String args[]) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("LagTimePost");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numParts);

        sqlContext.read().parquet(dataPath + "hid_hashtag.parquet").registerTempTable("ht");
        sqlContext.cacheTable("ht");

        String[] names = {"general", "disaster", "politic", "social"};
        for (String name: names) {
            sqlContext.read().parquet(outputPath + name + "Result.parquet").registerTempTable("lt");

            StringBuilder sb = new StringBuilder();
            sb.append("SELECT ht.hashtag ");
            for (int i = 0; i < sqlContext.table("lt").columns().length - 1; ++i) {
                sb.append(",avg(lagTime").append(i).append(") AS avgLagTime").append(i).append(" ");
            }
            sb.append("FROM lt, ht WHERE lt.hid = ht.hid GROUP BY ht.hashtag");

            sqlContext.sql(sb.toString()).coalesce(1).write().mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv").option("header", "true")
                .save(outputPath + name + "ResultAvg.csv");

            sqlContext.dropTempTable("lt");
        }
    }
}














