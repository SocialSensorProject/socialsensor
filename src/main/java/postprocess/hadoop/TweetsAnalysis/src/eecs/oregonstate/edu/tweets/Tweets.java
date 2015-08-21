/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eecs.oregonstate.edu.tweets;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.JSONParser;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 * This class implements the map method to 
 * extract the list of tweets from the preprocessed data.
 * 
 * @author rbouadjenek
 */
public class Tweets {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {

        private Text value = new Text();
        private LongWritable key = new LongWritable();
        private final JSONParser parser = new JSONParser();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Object obj;
            try {
                obj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) obj;
                long id = (Long) jsonObject.get("id");
                String screen_name = ((String) jsonObject.get("screen_name")).toLowerCase().trim();
                this.key = new LongWritable(id);
                this.value = new Text(screen_name);
                output.collect(this.key, this.value);
            } catch (ParseException ex) {
                Logger.getLogger(HashtagBirthday.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        JobConf conf = new JobConf(Tweets.class);
        conf.setJobName("Getting a list of tweets.");
        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Tweets.Map.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        JobClient.runJob(conf);
    }

}
