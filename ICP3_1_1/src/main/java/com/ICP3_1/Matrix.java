package com.ICP3_1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;

public class Matrix {

    public static class TokenizeMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            String line = value.toString();
            // (M, i, j, Mij);
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            if (indicesAndValue[0].equals("M")) {
                for (int k = 0; k < p; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k);
                    // outputKey.set(i,k);
                    outputValue.set(indicesAndValue[0] + "," + indicesAndValue[2]
                            + "," + indicesAndValue[3]);

                    context.write(outputKey, outputValue);
                }
            } else {
                // (N, j, k, Njk);
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]);
                    outputValue.set("N," + indicesAndValue[1] + ","
                            + indicesAndValue[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }


    }

    public static class Multiplier
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] value;

            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            for (Text val : values) {
                value = val.toString().split(",");
                if (value[0].equals("M")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));
                }
            }
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float m_ij;
            float n_jk;
            for (int j = 0; j < n; j++) {
                m_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f;
                n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += m_ij * n_jk;
            }
            if (result != 0.0f) {
                context.write(null,
                        new Text(key.toString() + "," + Float.toString(result)));
            }
        }

        public static void main(String[] args) throws Exception {
            if (args.length != 2) {
                System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
                System.exit(2);
            }
            Configuration conf = new Configuration();
            // M is an m-by-n matrix; N is an n-by-p matrix.
            conf.set("m", "1000");
            conf.set("n", "100");
            conf.set("p", "1000");
            @SuppressWarnings("deprecation")
            Job job = new Job(conf, "MatrixMultiply");
            job.setJarByClass(Matrix.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(TokenizeMapper.class);
            job.setReducerClass(Multiplier.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        }
    }
}
