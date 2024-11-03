package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Task3 {

    public static class UserActivityMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text userId = new Text();
        private LongWritable activeDays = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // 忽略第一行
            if (key.get() == 0) {
                return;
            }

            if (fields.length >= 9) {
                userId.set(fields[0]);
                double directPurchaseAmt = Double.parseDouble(fields[5]);
                double totalRedeemAmt = Double.parseDouble(fields[8]);

                // 如果金额大于0，则认为该用户活跃
                if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                    context.write(userId, activeDays);
                } else {
                    // 即使不活跃也输出，活跃天数为 0
                    context.write(userId, new LongWritable(0));
                }
            }
        }
    }

    public static class UserActivityReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private Map<Text, LongWritable> userActivityMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long activeDaysCount = 0;

            for (LongWritable val : values) {
                activeDaysCount += val.get();
            }

            userActivityMap.put(new Text(key), new LongWritable(activeDaysCount));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将结果按活跃天数降序排列
            List<Map.Entry<Text, LongWritable>> entryList = new ArrayList<>(userActivityMap.entrySet());
            Collections.sort(entryList, new Comparator<Map.Entry<Text, LongWritable>>() {
                @Override
                public int compare(Map.Entry<Text, LongWritable> o1, Map.Entry<Text, LongWritable> o2) {
                    return Long.compare(o2.getValue().get(), o1.getValue().get()); 
                }
            });

            // 输出结果
            for (Map.Entry<Text, LongWritable> entry : entryList) {
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Activity Calculation");
        job.setJarByClass(Task3.class);

        job.setMapperClass(UserActivityMapper.class);
        job.setReducerClass(UserActivityReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

