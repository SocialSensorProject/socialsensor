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
 * identify the hashtag birthday from the preprocessed data.
 * 
 * @author rbouadjenek
 */
public class HashtagBirthday {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private Text value = new Text();
        private final Text hashtag = new Text();
        private final JSONParser parser = new JSONParser();
        private final Extractor extractor = new Extractor();
        private final static DateFormat df1 = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzzz yyyy");
        private final static DateFormat df2 = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            Object obj;
            try {
                obj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) obj;
                String text = (String) jsonObject.get("text");
                String created_at = ((String) jsonObject.get("created_at")).trim();
                this.value = new Text(df2.format(df1.parse(created_at)) + "\t1");
                for (String ht : extractor.extractHashtags(text.toLowerCase())) {
                    this.hashtag.set("#" + ht.trim());
                    output.collect(this.hashtag, this.value);
                }
            } catch (ParseException | java.text.ParseException ex) {
                Logger.getLogger(HashtagBirthday.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        private final static DateFormat df = new SimpleDateFormat("yyy-MM-dd HH:mm:ss");

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            try {
                Date date1 = new Date(Long.MAX_VALUE);
                int sum = 0;
                while (values.hasNext()) {
                    StringTokenizer st = new StringTokenizer(values.next().toString(), "\t");
                    Date date2 = df.parse(st.nextToken());
                    sum += Integer.parseInt(st.nextToken());
                    if (date1.compareTo(date2) > 0) {
                        date1 = date2;
                    }
                }
                output.collect(key, new Text(df.format(date1) + "\t" + sum));
            } catch (java.text.ParseException ex) {
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
        JobConf conf = new JobConf(HashtagBirthday.class);
        conf.setJobName("Identifying hashtag birthdays");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(HashtagBirthday.Map.class);
//        conf.setCombinerClass(HashtagBirthday.Reduce.class);
        conf.setReducerClass(HashtagBirthday.Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf, new Path(args[2]));
        JobClient.runJob(conf);
    }

}
