package postprocess.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;
import preprocess.spark.ConfigRead;

import java.io.*;
import java.util.ArrayList;

public class PostProcessParquet implements Serializable {
    private static String outputCSVPath;
    private static ConfigRead configRead;
    private static boolean findTopMiddle = false;

    public static void loadConfig() throws IOException {
        configRead = new ConfigRead();
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        loadConfig();
        int itNum = configRead.getSensorEvalItNum();
        int hashtagNum = configRead.getSensorEvalHashtagNum();
        outputCSVPath = configRead.getOutputCSVPath();
        String scriptPath = configRead.getScriptPath();
        boolean local = configRead.isLocal();
        boolean calcNoZero = false;


        SparkConf sparkConfig;
        if(local) {
            outputCSVPath = "TestSet/output_all/";
            //outputCSVPath = "TestSet/Data/";
            sparkConfig = new SparkConf().setAppName("PostProcessParquet").setMaster("local[2]");
        }else
            sparkConfig = new SparkConf().setAppName("PostProcessParquet");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConfig);
        SQLContext sqlContext = new SQLContext(sparkContext);

        // Read all parquet part by part results files and combine them into 1 csv file for each iteration per group
        File folder = new File(outputCSVPath);
        ArrayList<String> fileNames = listFilesForFolder(folder);
        DataFrame res; int ind = 0;
        int[] lineNumbers = new int[fileNames.size()];
        for(String filename: fileNames) {
            System.out.println(outputCSVPath + "/" + filename);
            res = sqlContext.read().parquet(outputCSVPath + "/" + filename);
            lineNumbers[ind] = readResults2(res, sqlContext, ind, filename);
            ind++;
        }
        ind = 0;
        for(String filename: fileNames) {
            System.out.println("FileName: " + filename + " #lines: " + lineNumbers[ind]);
            ind++;
        }

        if(calcNoZero) {
            System.out.println("BUILDING NON_ZERO FILES");
            for (String filename : fileNames) {
                System.out.println(outputCSVPath + "/" + filename);
                lineNumbers[ind] = readResultsCSV(filename);
                ind++;
            }
        }

        // Combine all CSV files into one file for each group
        printForumla(itNum, hashtagNum);
        runScript("cp " + scriptPath + "mergeFiles.sh " + outputCSVPath + "mergeFiles.sh");
        runScript("chmod +x "+ outputCSVPath +"mergeFiles.sh");
        runScript("./"+ outputCSVPath +"mergeFiles.sh");
    }

    public static int readResults2(DataFrame results, SQLContext sqlContext, int index, String filename) throws IOException, InterruptedException {
        /**/

        JavaRDD strRes = results.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.get(1).toString() + "," + row.getString(0);
            }
        });
        strRes.coalesce(1).saveAsTextFile(outputCSVPath +"out_"+filename+"_csv");
        FileReader fileReaderA = new FileReader(outputCSVPath +"out_"+filename+"_csv/part-00000");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outputCSVPath +"CSVOut_"+filename+".csv");
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        while((line = bufferedReaderA.readLine()) != null){
            bw.write(line);
            bw.write("\n");
            numberOfLines++;
        }
        bw.close();
        if(findTopMiddle) {
            //=================== GET TOP MIDDLE BOTTOM===========
            runStringCommand("sed -n '1, " + configRead.getTopUserNum() + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "middle10_NoZero_" + filename);
            runStringCommand("sed -n '1, " + configRead.getTopUserNum() + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "middle10_NoZero_" + filename);
            runStringCommand("sed -n '1, " + configRead.getTopUserNum() + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "tail10_NoZero_" + filename);
        }
        return numberOfLines;
    }

    public static int readResultsCSV(String filename) throws IOException, InterruptedException {
        FileReader fileReaderA = new FileReader(outputCSVPath +filename);
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        FileWriter fw = new FileWriter(outputCSVPath +"NoZero_"+filename);
        BufferedWriter bw = new BufferedWriter(fw);
        int numberOfLines = 0;
        while((line = bufferedReaderA.readLine()) != null){
            if(line.split(",")[0].equals("0.0") || line.split(",")[0].equals("-0.0"))
                break;
            bw.write(line);
            bw.write("\n");
            numberOfLines++;
        }
        bw.close();
        if(findTopMiddle) {
            //=================== GET TOP MIDDLE BOTTOM===========
            runStringCommand("sed -n '" + ((int) Math.floor(numberOfLines / 2) - 5) + ", " + ((int) Math.floor(numberOfLines / 2) + 4) + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "middle10_NoZero_" + filename);
            runStringCommand("sed -n '" + (numberOfLines - 9) + ", " + numberOfLines + "p' " + outputCSVPath + "NoZero_" + filename + " >  " + outputCSVPath + "tail10_NoZero_" + filename);
        }
        System.out.println("Filename: " + filename + " #lines: " + numberOfLines);
        return numberOfLines;
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

    public static void runStringCommand(final String command) throws IOException, InterruptedException {
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














