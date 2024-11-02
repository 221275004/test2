package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Task4 {

    public static class CityMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text cityId = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 忽略第一行
            if (key.toString().equals("0")) {
                return;
            }
            
            String[] fields = value.toString().split(",");
            if (fields.length > 2) {
                // 获取城市代号（第3列）
                cityId.set(fields[2].trim());
                context.write(cityId, one);
            }
        }
    }

    public static class CityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<Text, IntWritable> cityCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            cityCountMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            cityCountMap.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().get() - e1.getValue().get())
                .forEach(entry -> {
                    try {
                        context.write(entry.getKey(), entry.getValue());
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Task4 <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "City Count");
        job.setJarByClass(Task4.class);
        job.setMapperClass(CityMapper.class);
        job.setReducerClass(CityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
