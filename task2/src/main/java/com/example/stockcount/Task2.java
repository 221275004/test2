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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Task2 {

    public static class FinancialMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final String[] DAYS = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\\s+|,");

            if (parts.length == 3) {
                String dateStr = parts[0];
                double income = Double.parseDouble(parts[1]);
                double outcome = Double.parseDouble(parts[2]);
                // 解析日期
                int year = Integer.parseInt(dateStr.substring(0, 4));
                int month = Integer.parseInt(dateStr.substring(4, 6));
                int day = Integer.parseInt(dateStr.substring(6, 8));
                LocalDate date = LocalDate.of(year, month, day);
                // 获取对应的星期
                int dayIndex = date.getDayOfWeek().getValue() - 1; 

                context.write(new Text(DAYS[dayIndex]), new Text(income + "," + outcome));
            }
        }
    }

    public static class FinancialReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, double[]> totals = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalIncome = 0.0;
            double totalOutcome = 0.0;
            int count = 0;
            for (Text value : values) {
                String[] incomeOutcome = value.toString().split(",");
                totalIncome += Double.parseDouble(incomeOutcome[0]);
                totalOutcome += Double.parseDouble(incomeOutcome[1]);
                count++;
            }
            // 计算平均值
            if (count > 0) {
                totals.put(key.toString(), new double[]{totalIncome / count, totalOutcome / count});
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ArrayList<Map.Entry<String, double[]>> entries = new ArrayList<>(totals.entrySet());
            entries.sort((entry1, entry2) -> Double.compare(entry2.getValue()[0], entry1.getValue()[0]));
            for (Map.Entry<String, double[]> entry : entries) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue()[0] + "," + entry.getValue()[1]));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Financial Analysis");
        job.setJarByClass(Task2.class);
        job.setMapperClass(FinancialMapper.class);
        job.setReducerClass(FinancialReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
