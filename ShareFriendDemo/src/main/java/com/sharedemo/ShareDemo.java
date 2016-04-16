package com.sharedemo;

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
 * 思路是找出拥有xx的共同好友集合（因为关系是单向的）
 * c  --> a b c d e f (假设，此种情况下，后面的人群都拥有c这个好友)
 * Created by Administrator on 2016/4/16.
 */
public class ShareDemo {
    static class ShareDemoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String res = value.toString();
            String[] field = res.split(":");
            String person = field[0];
            String friend = field[1];
            String[] friends = friend.split(",");
            // 输出<好友,人>
            for (String s : friends) {
                context.write(new Text(s), new Text(person));
            }
        }
    }

    static class ShareDemoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Text text : values) {
                sb.append(text.toString()).append(",");
            }
            sb.append("\r");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(ShareDemoMapper.class);
        job.setReducerClass(ShareDemoReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileInputFormat.setInputPaths(job, new Path("file:///D:/input")); // 我把我的文件放在了这个下面
        Path outPath = new Path("file:///D:/output"); // 输出路径在这下面
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        boolean bl = job.waitForCompletion(true);
        System.exit(bl ? 0 : 1);

        /**
         * 结果为：
         * A	I,K,C,B,G,F,H,O,D,
         B	A,F,J,E,
         C	A,E,B,H,F,G,K,
         D	G,C,K,A,L,F,E,H,
         E	G,M,L,H,A,F,B,D,
         F	L,M,D,C,G,A,
         G	M,
         H	O,
         I	O,C,
         J	O,
         K	B,
         L	D,E,
         M	E,F,
         O	A,H,I,J,F,
         */
    }
}
