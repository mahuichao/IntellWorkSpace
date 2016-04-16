package com.demo;

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
 *
 * //                            _ooOoo_
 * //                           o8888888o
 * //                           88" . "88
 * //                           (| -_- |)
 * //                            O\ = /O
 * //                        ____/`---'\____
 * //                      .   ' \\| |// `.
 * //                       / \\||| : |||// \
 * //                     / _||||| -:- |||||- \
 * //                       | | \\\ - /// | |
 * //                     | \_| ''\---/'' | |
 * //                      \ .-\__ `-` ___/-. /
 * //                   ___`. .' /--.--\ `. . __
 * //                ."" '< `.___\_<|>_/___.' >'"".
 * //               | | : `- \`.;`\ _ /`;.`/ - ` : | |
 * //                 \ \ `-. \_ __\ /__ _/ .-` / /
 * //         ======`-.____`-.___\_____/___.-`____.-'======
 * //                            `=---='
 * 将InputDemo中输出的结果作为本次的输入，进行加工处理
 * Created by Administrator on 2016/4/16.
 */
public class InputDemo02 {
    static class InputDemo02Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString();
            String[] fields = s.split("- -");
            context.write(new Text(fields[0]), new Text(fields[1]));
        }
    }

    static class InputDemo02Reducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text text : values) {
                sb.append(text + " ");
            }
            sb.append("\r");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(InputDemo02Mapper.class);
        job.setReducerClass(InputDemo02Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("file:///D:/output/"));
        FileSystem fileSystem = FileSystem.get(conf);
        Path outputPath = new Path("file:///D:/output02/");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
        /**
         * 结果为：
         * hello	c.txt	2 b.txt	2 a.txt	3
         jerry	c.txt	1 b.txt	3 a.txt	1
         tom  	c.txt	1 b.txt	1 a.txt	2
         */
    }
}
