/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eecs.oregonstate.edu.tweets;

import com.twitter.Extractor;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.json.simple.parser.JSONParser;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

/**
 * This class implements the map/reduce methods to 
 * extract for each tweet the hashtags it contains from the preprocessed data.
 * 
 * @author rbouadjenek
 */
public class TweetTokenizer {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private final Text key = new Text();
        private final JSONParser parser = new JSONParser();
        private final Extractor extractor = new Extractor();
        private final static DateFormat df1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzzz yyyy");
        private final static DateFormat df2 = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Object obj;
            try {
                obj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) obj;
                long id = (Long) jsonObject.get("id");
                String screen_name = ((String) jsonObject.get("screen_name")).toLowerCase().trim();
                String text = ((String) jsonObject.get("text")).toLowerCase();
                String created_at = ((String) jsonObject.get("created_at")).trim();
                for (String ht : extractor.extractHashtags(text)) {
                    this.key.set(id + "\t" + screen_name + "\t#" + ht.trim()+"\t"+df2.format(df1.parse(created_at)));
                    output.collect(this.key, this.one);
                }
            } catch (ParseException ex) {
                Logger.getLogger(HashtagBirthday.class.getName()).log(Level.SEVERE, null, ex);
            } catch (java.text.ParseException ex) {
                Logger.getLogger(TweetTokenizer.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        JobConf conf = new JobConf(TweetTokenizer.class);
        conf.setJobName("Tokenizing tweets.");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(TweetTokenizer.Map.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        JobClient.runJob(conf);
    }

}
