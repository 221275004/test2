package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Task1 {
    public static class BalanceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text date = new Text();
        private IntWritable amount = new IntWritable(); // 用于存储流入或流出金额

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // 忽略第一行
            if (key.get() == 0) {
                return;
            }

            String[] fields = line.split(",");
            if (fields.length > 9) {
                // 提取第2个、第5个和第9个字段
                String transactionTime = fields[1].trim();
                int income = Integer.parseInt(fields[4].trim());
                int outcome = Integer.parseInt(fields[8].trim());
                
                // 流入金额
                date.set(transactionTime);
                amount.set(income);
                context.write(date, new IntWritable(amount.get())); 
                
                // 流出金额
                amount.set(outcome);
                context.write(date, new IntWritable(-amount.get())); // 流出为正数，取负标记为流出
            }
        }
    }

    public static class BalanceReducer extends Reducer<Text, IntWritable, Text, Text> {
        private int totalPurchaseAmt; // 总流入
        private int totalRedeemAmt;   // 总流出

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            totalPurchaseAmt = 0;
            totalRedeemAmt = 0;
            
            // 对相同日期的资金流入和流出进行求和
            for (IntWritable value : values) {
                int amount = value.get();
                if (amount > 0) {
                    totalPurchaseAmt += amount; // 流入
                } else {
                    totalRedeemAmt += -amount; // 流出
                }
            }
            context.write(key, new Text(totalPurchaseAmt + "," + totalRedeemAmt));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Balance Analysis");
        job.setJarByClass(Task1.class);
        job.setMapperClass(BalanceMapper.class);
        job.setReducerClass(BalanceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
