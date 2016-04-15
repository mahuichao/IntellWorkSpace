package com.mhc.flow;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 我们的启动类，主要用来衔接mapper和reducer
 * 这里用的是本地模式来跑，并没有用yarn集群。
 * 其中具体区别不做过多介绍
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 指定我们自定义的数据分区
        job.setPartitionerClass(ProvincePartition.class);
        job.setNumReduceTasks(5);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("D:/goal.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:/output"));


        boolean bl = job.waitForCompletion(true);
        System.exit(bl ? 0 : 1);

    }

}
