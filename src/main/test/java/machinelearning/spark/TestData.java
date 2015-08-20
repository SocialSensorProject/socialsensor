package machinelearning.spark;

import org.json.simple.JSONObject;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by zahraiman on 8/14/15.
 */
public class TestData {
    private static String outputPath = "TestSet/";

    public static void main(String[] args) throws IOException {
        //Generate 100 tweets with 10 users and 12 hashtags.
        generateTestDataJson();
    }



    public static void generateTestDataJson() throws IOException {
        int numOfUsers = 10;
        int numOfHashtags = 12;
        int highUsage = 30;

        //{"screen_name":"avonsafety","created_at":"Fri Feb 28 13:00:00 +0000 2014","id":506187072,"text":"rt @kidrauhlsaussie: justin is a teenageri'm saying it while i can"}
        String tweetStr; int k;
        FileWriter file = new FileWriter(outputPath + "testset1.json");
        int tid = 0;
        for(int i = 0; i < numOfUsers; i++){
            JSONObject obj = new JSONObject();
            k = 0;
            for(int j = 0; j < 6; j++) {
                tid++;
                obj = new JSONObject();
                tweetStr = "abcde #h" + (i + 1) + " @user" + ((i + k + 1) % numOfUsers) + "}";
                k++;
                obj.put("screen_name", "user" + (i+1));
                obj.put("created_at", "Fri Feb 28 " + (10 + j) + ":00:00 +0000 "+(2013+j%2));
                obj.put("id", tid);
                obj.put("text", tweetStr);
                file.write(obj.toJSONString());
                file.write("\n");
                file.flush();
            }
            for(int j = 0; j < 3; j++) {
                tid++;
                obj = new JSONObject();
                tweetStr = "abcde #h" + ((i+1)%numOfHashtags+1) + " @user" + ((i + k + 1) % numOfUsers+1) + "}";
                k++;
                obj.put("screen_name", "user" + (i+1));
                obj.put("created_at", "Fri Feb 28 " + (10 + j) + ":00:00 +0000 "+(2013+j%2));
                obj.put("id", tid);
                obj.put("text", tweetStr);
                file.write(obj.toJSONString());
                file.write("\n");
                file.flush();
            }
            tid++;
            obj = new JSONObject();
            tweetStr = "abcde #h" + ((i+2)%numOfHashtags+1) + " @user" + ((i + k + 1) % numOfUsers+1) + "}";
            k++;
            obj.put("screen_name", "user" + (i+1));
            obj.put("created_at", "Fri Feb 28 " + (10 + i) + ":00:00 +0000 "+(2013+i%2));
            obj.put("id", tid);
            obj.put("text", tweetStr);
            file.write(obj.toJSONString());
            file.write("\n");
            file.flush();
        }
        file.close();
        //.mode(SaveMode.Overwrite).json("TestSet/testset1_json");
    }
}
