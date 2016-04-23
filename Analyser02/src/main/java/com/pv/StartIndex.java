package com.pv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/23.
 */
public class StartIndex {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(PageViewsMapper.class);
        job.setReducerClass(PageViewsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PvLoggers.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PvLoggers.class);

        FileInputFormat.setInputPaths(job, new Path("D:/input"));
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath = new Path("D:/output");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
