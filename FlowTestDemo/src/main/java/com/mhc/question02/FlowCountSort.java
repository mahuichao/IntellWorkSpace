package com.mhc.question02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/14.
 */
public class FlowCountSort {
    static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
        FlowBean bean = new FlowBean();
        Text v = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String phoneNbr = fields[0];
            long upFlow = Long.parseLong(fields[1]);
            long dFlow = Long.parseLong(fields[2]);
            bean.set(upFlow, dFlow);
            v.set(phoneNbr);
            context.write(bean, v);
        }
    }

    static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
        @Override
        protected void reduce(FlowBean fb, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(values.iterator().next(), fb);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 采用本地运行模式
         */
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCountSort.class);
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("/FlowInput/"));
        Path outPath = new Path("/FlowInput/output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean bl = job.waitForCompletion(true);
        System.exit(bl ? 0 : 1);

    }
}
