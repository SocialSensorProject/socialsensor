package preprocess.spark;

import java.io.*;
import java.util.Properties;

/**
 * Created by zahraiman on 8/18/15.
 */
public class ConfigRead {
    private Properties prop;

    public ConfigRead() throws IOException {
        getPropValues();
    }

    public void getPropValues() throws IOException {
        prop = new Properties();
        //String propFileName = "src/main/resources/config/config.properties";
        String propFileName = "/zaracode/classes/resources/config/config.properties";
        try {
            if(!new File(propFileName).exists())
                propFileName = "src/main/resources/config/config.properties"; // This is for local run
            FileReader fileReader = new FileReader(propFileName);
            BufferedReader inputStream = new BufferedReader(fileReader);

            //inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            prop.load(inputStream);
        } catch (Exception e) {
            throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
        }
    }

    public boolean isUniqueUserHashtagBirthday() {
        return Boolean.valueOf(prop.getProperty("uniqueUserHashtagBirthday"));
    }

    public boolean isDirectedUserNet() {
        return Boolean.valueOf(prop.getProperty("directedUserNet"));
    }

    public boolean isGroupedUserMention() {
        return Boolean.valueOf(prop.getProperty("groupedUserMention"));
    }

    public boolean isTweetUser() {
        return Boolean.valueOf(prop.getProperty("tweetUser"));
    }

    public boolean isTweetUserHashtag() {
        return Boolean.valueOf(prop.getProperty("tweetUserHashtag"));
    }

    public boolean isGroupedTweetUserHashtag() {
        return Boolean.valueOf(prop.getProperty("tweetUserHashtagGrouped"));
    }

    public boolean isLocal() {
        return Boolean.valueOf(prop.getProperty("local"));
    }

    public boolean isTweetHashtagTime(){
        return Boolean.valueOf(prop.getProperty("tweetHashtagTime"));
    }

    public Properties getProp() {
        return prop;
    }

    public String getHdfsPath(){
        return prop.getProperty("hdfsPath");
    }

    public String getDataPath() {
        return prop.getProperty("dataPath");
    }
    public String getOutputPath() {
        return prop.getProperty("outputPath");
    }
    public int getNumPart() {
        return Integer.valueOf(prop.getProperty("numPart"));
    }

    public String getOutputCSVPath(){
        return prop.getProperty("outputCSVPath");
    }

    public int getSensorEvalHashtagNum(){
        return Integer.valueOf(prop.getProperty("sensorEvalHashtagNum"));
    }

    public int getSensorEvalItNum(){
        return Integer.valueOf(prop.getProperty("sensorEvalItNum"));
    }

    public int getTopUserNum(){
        return Integer.valueOf(prop.getProperty("topNumUsers"));
    }

    public String getTestDataPath(){
        return  prop.getProperty("testDataPath");
    }
    public String getTestOutPath(){
        return  prop.getProperty("testOutPath");
    }

    public boolean isTweetMention() {
        return Boolean.valueOf(prop.getProperty("tweetMention"));
    }

    public boolean isTweetUserMention() {
        return Boolean.valueOf(prop.getProperty("tweetUserMention"));
    }

    public String getScriptPath() { return prop.getProperty("scriptPath"); }

    public boolean isGroupedTweetHashtagHashtag() {
        return Boolean.valueOf(prop.getProperty("tweetHashtagHashtagGrouped"));
    }

    public boolean isGroupedTweetMentionHashtag() {
        return Boolean.valueOf(prop.getProperty("tweetMentionHashtagGrouped"));
    }

    public boolean isGroupedTweetTermHashtag() {
        return Boolean.valueOf(prop.getProperty("tweetTermHashtagGrouped"));
    }

    public String getOutputJsonPath() { return prop.getProperty("outputJsonPath");
    }

    public boolean getHashtagUserFeatures(){ return Boolean.valueOf(prop.getProperty("hashtagUserFeatures"));}
    public int getUserLocThreshold(){ return Integer.valueOf(prop.getProperty("userLocThreshold"));}

    public boolean getTermFeatures() {
        return Boolean.valueOf(prop.getProperty("termFeatures"));
    }
    public boolean getTestTrainData() {
        return Boolean.valueOf(prop.getProperty("buildTestTrainData"));
    }

    public boolean isTweetTime() { return Boolean.valueOf(prop.getProperty("tweetTime"));}

    public boolean isWriteHashtagSetBirthday() { return Boolean.valueOf(prop.getProperty("writeHashtagSetBirthday"));}

    public boolean isHashtagBirthdays() {
        return Boolean.valueOf(prop.getProperty("hashtagPairBirthday"));
    }

    public boolean isUserFeatures() { return Boolean.valueOf(prop.getProperty("userFeatures")); }

    public String[] getGroupNames() { return prop.getProperty("groupNames").split(",");  }

    public int getNumOfGroups(){
        return Integer.valueOf(prop.getProperty("numOfGroups"));
    }

    public String getLearningPath() { return prop.getProperty("learningPath"); }

    public double getTrainPercentage(){
        return Double.valueOf(prop.getProperty("trainPercentage"));
    }

    public boolean getTestFlag() {
        return Boolean.valueOf(prop.getProperty("testFlag"));
    }

    public boolean findTermStats() {
        return Boolean.valueOf(prop.getProperty("findTermStat"));
    }

    public boolean getText() {
        return Boolean.valueOf(prop.getProperty("getText"));
    }
}
