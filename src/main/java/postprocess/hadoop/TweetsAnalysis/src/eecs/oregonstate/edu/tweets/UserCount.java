/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package postprocess.hadoop.TweetsAnalysis.src.eecs.oregonstate.edu.tweets;

import java.io.IOException;
import java.util.*;
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
 * identify the unique users in the preprocessed data.
 * @author rbouadjenek
 */
public class UserCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text screen_name = new Text();
        private final JSONParser parser = new JSONParser();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Object obj;
            try {
                obj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) obj;
                String sn = ((String) jsonObject.get("screen_name")).toLowerCase().trim();
                this.screen_name.set(sn);
                output.collect(this.screen_name, Map.one);
            } catch (ParseException ex) {
                Logger.getLogger(UserCount.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    /**
     * @param args the command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        JobConf conf = new JobConf(UserCount.class);
        conf.setJobName("Identifying uniq users");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        JobClient.runJob(conf);
    }

}
