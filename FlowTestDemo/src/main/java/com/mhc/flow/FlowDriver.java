package com.mhc.flow;


import com.mhc.partition.ProvincePartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {
    /**
     *
     * @param args
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//        job.setJarByClass(FlowDriver.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 指定我们自定义的数据分区
        job.setPartitionerClass(ProvincePartition.class);        // 同时指定我们自定义的数据分区数量的reducetask
        job.setNumReduceTasks(5);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\传智播客文档\\hadoop\\day08\\作业题\\goal.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:/output"));


        boolean bl = job.waitForCompletion(true);
        System.exit(bl ? 0 : 1);

    }

}
