package postprocess.spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by imanz on 11/3/15.
 */
public class TrecEvaluation {

    public static int readTrecResults(String outPath, String filename, BufferedWriter bw, String topic, String folderName) throws IOException, InterruptedException {
        /**/
        filename = filename.split("_")[2];
        FileReader fileReaderA = new FileReader(outPath + "out_" + filename + ".csv");
        BufferedReader bufferedReaderA = new BufferedReader(fileReaderA);
        String line;
        int ind = 0;
        bw.write(topic + "," + folderName + "," + filename + ",");
        while((line = bufferedReaderA.readLine()) != null){
            ind++;
            if(ind == 2)
                bw.write(line.split("num_ret        \tall\t")[1] + ",");
            if(ind == 3)
                bw.write(line.split("num_rel        \tall\t")[1] + ",");
            if(ind == 5)
                bw.write(line.split("map            \tall\t")[1] + ",");
            if(ind == 20)
                bw.write(line.split("map_at_R       \tall\t")[1] + ",");
            if(ind == 60)
                bw.write(line.split("P100           \tall\t")[1] + ",");
            if(ind == 63)
                bw.write(line.split("P1000          \tall\t")[1] + ",");
        }
        bw.write("\n");
        bw.flush();
        bufferedReaderA.close();
        return 0;
    }
}
