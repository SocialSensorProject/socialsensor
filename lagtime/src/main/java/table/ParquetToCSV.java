package table;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class ParquetToCSV implements Serializable {
    private static String dataPath = "output/";
    private static String outputPath = "output/";

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setAppName("ParquetToCSV");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + Parquet.numPart);

        // convert tables to csv
        output(sqlContext.read().parquet(dataPath + "hid_hashtag.parquet"), "hid_hashtag.csv");
        output(sqlContext.read().parquet(dataPath + "uid_uname.parquet"), "uid_uname.csv");
        output(sqlContext.read().parquet(dataPath + "tid_hid.parquet"), "tid_hid.csv");
        output(sqlContext.read().parquet(dataPath + "tid_mid.parquet"), "tid_mid.csv");

        // timestamp: SecSinceEpoch -> ISO-8601
        JavaRDD<Row> rdd = sqlContext.read().parquet(dataPath + "tid_uid_ts.parquet").javaRDD().map(
            new Function<Row, Row>() {
                @Override
                public Row call(Row r) throws Exception {
                    SimpleDateFormat sdf = new SimpleDateFormat(
                        "yyyy-MM-dd'T'HH:mm:ssXXX" // ISO-8601
                    );
                    sdf.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));

                    return RowFactory.create(
                        r.getLong(0),
                        r.getLong(1),
                        sdf.format(new Date(r.getLong(2) * 1000))
                    );
                }
            }
        );
        StructField[] fields = {
            DataTypes.createStructField("tid", DataTypes.LongType, false),
            DataTypes.createStructField("uid", DataTypes.LongType, false),
            DataTypes.createStructField("ts", DataTypes.StringType, false)
        };
        output(sqlContext.createDataFrame(
            rdd,
            DataTypes.createStructType(fields)
        ), "tid_uid_ts.csv");
    }

    private static void output(DataFrame df, String folderName) {
        df.write().format("com.databricks.spark.csv").save(outputPath + folderName);
    }
}








