package table;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import com.twitter.Extractor;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parquet implements Serializable {
    private static String dataPath = "data/";
    private static String outputPath = "output/";

    private static Extractor hmExtractor = new Extractor();
    private static StorageLevel storageLevel = StorageLevel.MEMORY_ONLY();
    protected static int numPart = 640;

    public static void main(String args[]) {
        SparkConf sparkConf = new SparkConf().setAppName("Parquet");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        sqlContext.sql("SET spark.sql.shuffle.partitions=" + numPart);

        // read data
        DataFrame noText = sqlContext.read().json(dataPath + "*.bz2").repartition(numPart);
        DataFrame id_text = noText.drop("created_at").drop("screen_name");
        noText = noText.drop("text");
        noText = noText.persist(storageLevel);
/*
        // hashtags
        DataFrame mix = extractHM(id_text, true, "tid", "hashtag");
        DataFrame en = getEntity(mix.drop("tid"), "hid", "hashtag");
        mix = mix.persist(storageLevel);
        en = en.persist(storageLevel);
        output(en, "hid_hashtag.parquet");
        output(getRelationship(
            mix, en,
            new Tuple2<>("tid", "tid"), "hashtag",
            new Tuple2<>("hid", "hid"), "hashtag"
        ), "tid_hid.parquet");
        mix.unpersist();
        en.unpersist();

        // users
        mix = extractHM(id_text, false, "tid", "mention");
        en = getEntity(
            noText.drop("id").drop("created_at").unionAll(mix.drop("tid"))
            , "uid", "uname"
        );
        mix = mix.persist(storageLevel);
        en = en.persist(storageLevel);
        output(en, "uid_uname.parquet");
        output(getRelationship(
            mix, en,
            new Tuple2<>("tid", "tid"), "mention",
            new Tuple2<>("uid", "mid"), "uname"
        ), "tid_mid.parquet");

        mix.unpersist();
        //en.unpersist(); //keep for next computation

        // tweets
        output(getTweetTable(noText, en), "tid_uid_ts.parquet");
*/
        // terms
        DataFrame tid_term = getTidTerm(id_text).persist(storageLevel);
        JavaRDD<Row> tmp = tid_term.drop("tid").distinct().javaRDD().zipWithIndex().map(
            new Function<Tuple2<Row, Long>, Row>() {
                @Override
                public Row call(Tuple2<Row, Long> tuple) throws Exception {
                    return RowFactory.create(tuple._2(), tuple._1().getString(0));
                }
            }
        );
        StructField[] fields = {
            DataTypes.createStructField("teid", DataTypes.LongType, false),
            DataTypes.createStructField("term", DataTypes.StringType, false)
        };
        DataFrame terms = sqlContext.createDataFrame(tmp, DataTypes.createStructType(fields));
        terms.persist(storageLevel);
        output(terms, dataPath + "teid_term.parquet");

        tid_term.registerTempTable("tid_term");
        terms.registerTempTable("terms");
        output(
            sqlContext.sql(
                "SELECT tt.tid, t.teid" +
                    " FROM tid_term AS tt, terms AS t" +
                    " WHERE tt.term = t.term"
            ),
            dataPath + "tid_teid.parquet"
        );
        sqlContext.dropTempTable("tid_term");
        sqlContext.dropTempTable("terms");
    }

    private static DataFrame extractHM(DataFrame id_text, final boolean hashtag, String idCol, String nameCol) {
        JavaRDD<Row> rdd = id_text.javaRDD().flatMap(
            new FlatMapFunction<Row, Row>() {
                @Override
                public Iterable<Row> call(Row row) throws Exception {
                    Iterable<String> tmp = (
                        hashtag ?
                            hmExtractor.extractHashtags(row.getString(1)) :
                            hmExtractor.extractMentionedScreennames(row.getString(1))
                    );

                    long id = row.getLong(0);
                    LinkedList<Row> list = new LinkedList<>();
                    for (String word : tmp) {
                        list.add(RowFactory.create(id, word.toLowerCase(Locale.US)));
                    }

                    return list;
                }
            }
        );

        StructField[] fields = {
            DataTypes.createStructField(idCol, DataTypes.LongType, false),
            DataTypes.createStructField(nameCol, DataTypes.StringType, false)
        };
        return id_text.sqlContext().createDataFrame(
            rdd,
            DataTypes.createStructType(fields)
        );
    }

    private static DataFrame getEntity(DataFrame df, String idCol, String nameCol) {
        JavaRDD<Row> rdd = df.javaRDD().map(
            new Function<Row, String>() {
                @Override
                public String call(Row row) throws Exception {
                    return row.getString(0).toLowerCase(Locale.US);
                }
            }
        ).distinct().zipWithIndex().map(
            new Function<Tuple2<String, Long>, Row>() {
                @Override
                public Row call(Tuple2<String, Long> tuple) throws Exception {
                    return RowFactory.create(tuple._2(), tuple._1());
                }
            }
        );
        StructField[] fields = {
            DataTypes.createStructField(idCol, DataTypes.LongType, false),
            DataTypes.createStructField(nameCol, DataTypes.StringType, false)
        };
        return df.sqlContext().createDataFrame(
            rdd,
            DataTypes.createStructType(fields)
        );
    }

    private static DataFrame getRelationship(DataFrame mix, DataFrame en,
                                             Tuple2<String, String> idMix, String nameMix,
                                             Tuple2<String, String> idEn, String nameEn) {
        SQLContext sqlContext = mix.sqlContext();
        mix.registerTempTable("mix");
        en.registerTempTable("en");
        DataFrame rv = sqlContext.sql(
            "SELECT DISTINCT " + idMix._1() + " AS " + idMix._2() + ", " + idEn._1() + " AS " + idEn._2() +
                " FROM mix, en" +
                " WHERE mix." + nameMix + " = en." + nameEn
        );
        sqlContext.dropTempTable("mix");
        sqlContext.dropTempTable("en");

        return rv;
    }

    private static DataFrame getTweetTable(DataFrame tweets, DataFrame users) {
        SQLContext sqlContext = tweets.sqlContext();

        // parse 'created_at' & convert 'user' to lowercase
        JavaRDD<Row> rdd = tweets.javaRDD().map(
            new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    // sdf not thread-safe
                    SimpleDateFormat sdf = new SimpleDateFormat(
                        "EEE MMM dd HH:mm:ss Z yyyy" // "Thu Jan 31 12:58:06 +0000 2013"
                    );

                    long epochSec = sdf.parse(row.getString(0)).getTime() / 1000;
                    return RowFactory.create(row.getLong(1), row.getString(2).toLowerCase(Locale.US), epochSec);
                }
            }
        );
        StructField[] fields = {
            DataTypes.createStructField("tid", DataTypes.LongType, false),
            DataTypes.createStructField("uname", DataTypes.StringType, false),
            DataTypes.createStructField("ts", DataTypes.LongType, false)
        };
        tweets = sqlContext.createDataFrame(
            rdd,
            DataTypes.createStructType(fields)
        );

        // replace 'user' with 'uid'
        tweets.registerTempTable("t");
        users.registerTempTable("u");
        DataFrame rv = sqlContext.sql(
            "SELECT tid, uid, ts" +
                " FROM t, u" +
                " WHERE t.uname = u.uname"
        );
        sqlContext.dropTempTable("t");
        sqlContext.dropTempTable("u");

        return rv;
    }

    public static DataFrame getTidTerm(DataFrame tweet_text) {
        final String emo_regex2 = "([\\u20a0-\\u32ff\\ud83c\\udc00-\\ud83d\\udeff\\udbb9\\udce5-\\udbb9\\udcee])"; //"\\p{InEmoticons}";
        JavaRDD <Row> t1 = tweet_text.javaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterable<Row> call(Row row) throws Exception {
                long id = row.getLong(0);
                ArrayList<Row> list = new ArrayList<>();
                Matcher matcher = Pattern.compile(emo_regex2).matcher(row.getString(1));
                while(matcher.find())
                    list.add(RowFactory.create(id, matcher.group().toLowerCase(Locale.US)));
                String text = matcher.replaceAll("").trim();
                StringTokenizer stok = new StringTokenizer(text, "\'\"?, ;.:!()-*ág|><`~$^&[]\\}{=h?fceIŒF+L");
                String str = "";
                boolean write = true, isUrl = false, containHttp = false;
                while(stok.hasMoreTokens()){
                    write = true;
                    str = stok.nextToken();
                    while(containHttp || str.contains("@") || str.contains("#") || str.contains("http")) {//"#that#this@guy did "
                        if(containHttp){
                            while (str.contains("/")) {
                                if (!stok.hasMoreTokens()) {
                                    write = false;
                                    break;
                                }
                                str = stok.nextToken();
                            }
                            containHttp = false;
                        }
                        isUrl = str.startsWith("http");
                        if(!isUrl) {
                            if(str.contains("http")){
                                containHttp = true;
                                if(str.split("http")[0].length() > 0){
                                    str = str.split("http")[0];
                                    if (str.length() == 0)
                                        write = false;
                                }else
                                    write = false;
                            }if(str.contains("@") || str.contains("#")) {
                                if (str.split("[@#]").length > 0) {
                                    str = str.split("[@#]")[0];
                                    if (str.length() == 0)
                                        write = false;
                                } else
                                    write = false;
                            }
                            break;
                        }
                        if(!stok.hasMoreTokens()) {
                            write = false;
                            break;
                        }else {
                            str = stok.nextToken();
                            if (isUrl) {
                                while (str.contains("/")) {
                                    if (!stok.hasMoreTokens()) {
                                        write = false;
                                        break;
                                    }
                                    str = stok.nextToken();
                                }
                            }
                        }
                    }
                    if(write) {
                        if(str.contains("/")) {
                            for(String st: str.split("/")) {
                                list.add(RowFactory.create(id, st.toLowerCase(Locale.US)));
                            }
                        }else {
                            list.add(RowFactory.create(id, str.toLowerCase(Locale.US)));
                        }
                    }
                }
                return list;
            }
        });
        StructField[] fields = {
            DataTypes.createStructField("tid", DataTypes.LongType, false),
            DataTypes.createStructField("term", DataTypes.StringType, false),
        };
        return tweet_text.sqlContext().createDataFrame(t1, DataTypes.createStructType(fields)).distinct();
    }

    private static void output(DataFrame data, String folderName) {
        data.write().parquet(outputPath + folderName);
    }
}














