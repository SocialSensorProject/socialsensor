package edu.oregonstate.spark.outputfilter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class ReadOutputParquets implements Serializable {
    private static String dataPath = "output_parquets/";
    private static String midDataPath = "output_csv_dist/";
    private static String outPath = "output_csv_files/";

    //THIS IS DONE ON LOCAL SYSTEM AFTER THE OUTPUT PARQUET FILES ARE COMPUTED

    public static void main(String args[]) throws IOException {
        SparkConf sparkConfig = new SparkConf().setAppName("ReadOutputParquets").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);

        String path = dataPath;
        File folder = new File(path);
        ArrayList<String> fileNames = listFilesForFolder(folder);
        DataFrame res; int ind = 0;
        for(String filename: fileNames) {
            System.out.println(path + "/" + filename);
            res = sqlContext.read().parquet(path + "/" +  filename);
            readResults2(res, sqlContext, ind, filename);
            ind++;
        }
    }

    public static void readResults2(DataFrame results, SQLContext sqlContext, int index, String filename) throws IOException {
        /**/

        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                String str = row.getString(0) + "," + row.get(1).toString();
                return str;
            }
        });
        strRes.coalesce(1).saveAsTextFile(midDataPath + filename + "_csv");
        FileReader fileReaderA = new FileReader(midDataPath +filename+"_csv/part-00000");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outPath+filename+".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        while((line = bufferedReaderA.readLine()) != null){
            bw.write(line);
            bw.write("\n");
        }
        bw.close();
    }


    public static ArrayList<String> listFilesForFolder(final File folder) {
        ArrayList<String> fileNames = new ArrayList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                fileNames.add(fileEntry.getName());
                System.out.println(fileEntry.getName());
            } else {
                if(!fileEntry.getName().startsWith(".") && !fileEntry.getName().startsWith("_")) {
                    fileNames.add(fileEntry.getName());
                    System.out.println(folder.getPath() + "/" +  fileEntry.getName());
                }
            }
        }
        return fileNames;
    }

}














