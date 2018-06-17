package com.lab1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class lab1 {

    //Mapper Class
    public static class FriendMapper
                extends Mapper<LongWritable, Text, Text, Text> {

        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(">"); //Splitting the input at ">"
            if (line.length == 2) {
                String f1 = line[0]; //Getting user name.
                List<String> values = Arrays.asList(line[1].split(",")); //Splitting friends of the user.
                for (String f2 : values) { //Getting each friend.

                    if (Integer.parseInt(f1) < Integer.parseInt(f2))
                        word.set(f1 + "," + f2); //Setting word as mapping output.
                    else
                        word.set(f2 + "," + f1);
                    context.write(word, new Text(line[1]));
                }
            }
        }

    }



    //Reducer Class
    public static class friendsReducer extends Reducer<Text, Text, Text, Text>
    {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> friendMap = new HashMap<String, Integer>();
            StringBuilder sb = new StringBuilder();
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().split(","));
                for (String friend : temp) {
                    if (friendMap.containsKey(friend))
                        sb.append(friend + ',');
                    else
                        friendMap.put(friend, 1);

                }
            }
            if (sb.lastIndexOf(",") > -1) {
                sb.deleteCharAt(sb.lastIndexOf(","));
            }

            result.set(new Text(sb.toString()));
            context.write(key, result); //Generating the reduced output.
        }
    }


        //Driver Class
        public static void main(String[] args) throws Exception {
            if (args.length != 2) { //If number of args is not 2, fail
                System.err.println("Usage: Mutual Friends");
                System.exit(2);
            }
            Configuration conf = new Configuration();

            @SuppressWarnings("deprecation")
            Job job = new Job(conf, "MutualFriends");
            job.setJarByClass(lab1.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(FriendMapper.class);
            job.setReducerClass(friendsReducer.class);

            FileInputFormat.addInputPath(job, new Path(args[0])); //Input
            FileOutputFormat.setOutputPath(job, new Path(args[1])); //Output

            job.waitForCompletion(true);

        }


    }