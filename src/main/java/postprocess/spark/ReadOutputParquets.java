package sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class ReadOutputParquets implements Serializable {
    private static String dataPath = "Cluster_results/clusterResults/";

    public static void main(String args[]) throws IOException, InterruptedException {
        int itNum = 20;
        int hashtagNum = 1044;

        SparkConf sparkConfig = new SparkConf().setAppName("ReadTable").setMaster("local[2]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);

        // Read all parquet part by part results files and combine them into 1 csv file for each iteration per group
        String path = "Cluster_results/clusterResults/";
        File folder = new File(path);
        ArrayList<String> fileNames = listFilesForFolder(folder);
        DataFrame res; int ind = 0;
        for(String filename: fileNames) {
            System.out.println(path + "/" + filename);
            res = sqlContext.read().parquet(path + "/" +  filename);
            readResults2(res, sqlContext, ind, filename);
            ind++;
        }
        // Combine all CSV files into one file for each group
        printForumla(itNum, hashtagNum);
        runScript("cp script/script2.sh Cluster_results/clusterResults/script2.sh");
        runScript("chmod +x Cluster_results/clusterResults/script2.sh");
        runScript("./Cluster_results/clusterResults/script2.sh");
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
        strRes.coalesce(1).saveAsTextFile(dataPath+"out_"+filename+"_csv");
        FileReader fileReaderA = new FileReader(dataPath+"out_"+filename+"_csv/part-00000");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(dataPath+"CSVOut_"+filename+".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        while((line = bufferedReaderA.readLine()) != null){
            bw.write(line);
            bw.write("\n");
        }
        bw.close();
    }

    public static void readResults() throws IOException {
        String path = "res/";
        final File folder = new File(path);
        ArrayList<String> fileNames = listFilesForFolder(folder);
        BufferedReader bufferedReader;
        FileWriter fileWriterA = new FileWriter("res/Group_A.txt");
        BufferedWriter bufferedWriterA = new BufferedWriter(fileWriterA);
        FileWriter fileWriterB = new FileWriter("res/Group_B.txt");
        BufferedWriter bufferedWriterB = new BufferedWriter(fileWriterB);
        FileWriter fileWriterC = new FileWriter("res/Group_C.txt");
        BufferedWriter bufferedWriterC = new BufferedWriter(fileWriterC);
        FileWriter fileWriterD = new FileWriter("res/Group_D.txt");
        BufferedWriter bufferedWriterD = new BufferedWriter(fileWriterD);
        FileReader fileReader;
        String line;
        for(String filename : fileNames) {
            fileReader = new FileReader(filename);
            bufferedReader = new BufferedReader(fileReader);
            bufferedWriterA.write(bufferedReader.readLine());
            bufferedWriterB.write(bufferedReader.readLine());
            bufferedWriterC.write(bufferedReader.readLine());
            bufferedWriterD.write(bufferedReader.readLine());
        }
        bufferedWriterA.close();
        bufferedWriterB.close();
        bufferedWriterC.close();
        bufferedWriterD.close();
    }

    public static ArrayList<String> listFilesForFolder(final File folder) {
        ArrayList<String> fileNames = new ArrayList<String>();
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                fileNames.add(fileEntry.getName());
                System.out.println(fileEntry.getName());
                //listFilesForFolder(fileEntry);
            } else {
                if(!fileEntry.getName().startsWith(".") && !fileEntry.getName().startsWith("_")) {
                    fileNames.add(fileEntry.getName());
                    System.out.println(folder.getPath() + "/" +  fileEntry.getName());
                }
            }
        }
        return fileNames;
    }

    /**
     * Run script.
     *
     * @param scriptFile the script file
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InterruptedException the interrupted exception
     */
    public static void runScript(final String scriptFile) throws IOException, InterruptedException {
        final String command = scriptFile;
        if (!new File(command).exists() || !new File(command).canRead() || !new File(command).canExecute()) {
            System.err.println("Cannot find or read " + command);
            System.err.println("Make sure the file is executable and you have permissions to execute it. Hint: use \"chmod +x filename\" to make it executable");
            throw new IOException("Cannot find or read " + command);
        }
        final int returncode = Runtime.getRuntime().exec(new String[] { "bash", "-c", command }).waitFor();
        if (returncode != 0) {
            System.err.println("The script returned an Error with exit code: " + returncode);
            throw new IOException();
        }
    }

    public static void printForumla(int itNum, int hashtagNum){
        String str = "=AVERAGE(";
        for(int i = 1; i <= itNum; i++)
            str += "B" + String.valueOf((i-1)*hashtagNum + 1) + ",";
        str += ")";
        System.out.println(str);
    }
}