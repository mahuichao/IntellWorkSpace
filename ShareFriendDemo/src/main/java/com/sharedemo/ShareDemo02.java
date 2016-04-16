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
import java.util.Arrays;

/**
 * Created by Administrator on 2016/4/16.
 */
public class ShareDemo02 {
    static class ShareDemo02Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String res = value.toString();
            String[] friend_person = res.split("\t");
            String com_friend = friend_person[0];
            String person = friend_person[1];
            String[] persons = person.split(",");
            Arrays.sort(persons); // 对其进行排序，防止B C 和 C B 识别为两个，其实应该归结为一个

            for (int i = 0; i < persons.length - 1; i++)
                for (int j = i + 1; j < persons.length; j++) {
                    context.write(new Text(persons[i] + "--" + persons[j]), new Text(com_friend));
                }
        }
    }

    static class ShareDemo02Reducer extends Reducer<Text, Text, Text, Text> {
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
        job.setMapperClass(ShareDemo02Mapper.class);
        job.setReducerClass(ShareDemo02Reducer.class);


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
         * 最后的结果为：
         * A--B	E,C,
         A--C	D,F,
         A--D	E,F,
         A--E	D,B,C,
         A--F	O,B,C,D,E,
         A--G	F,E,C,D,
         A--H	E,C,D,O,
         A--I	O,
         A--J	O,B,
         A--K	D,C,
         A--L	F,E,D,
         A--M	E,F,
         B--C	A,
         B--D	A,E,
         B--E	C,
         B--F	E,A,C,
         B--G	C,E,A,
         B--H	A,E,C,
         B--I	A,
         B--K	C,A,
         B--L	E,
         B--M	E,
         B--O	A,
         C--D	A,F,
         C--E	D,
         C--F	D,A,
         C--G	D,F,A,
         C--H	D,A,
         C--I	A,
         C--K	A,D,
         C--L	D,F,
         C--M	F,
         C--O	I,A,
         D--E	L,
         D--F	A,E,
         D--G	E,A,F,
         D--H	A,E,
         D--I	A,
         D--K	A,
         D--L	E,F,
         D--M	F,E,
         D--O	A,
         E--F	D,M,C,B,
         E--G	C,D,
         E--H	C,D,
         E--J	B,
         E--K	C,D,
         E--L	D,
         F--G	D,C,A,E,
         F--H	A,D,O,E,C,
         F--I	O,A,
         F--J	B,O,
         F--K	D,C,A,
         F--L	E,D,
         F--M	E,
         F--O	A,
         G--H	D,C,E,A,
         G--I	A,
         G--K	D,A,C,
         G--L	D,F,E,
         G--M	E,F,
         G--O	A,
         H--I	O,A,
         H--J	O,
         H--K	A,C,D,
         H--L	D,E,
         H--M	E,
         H--O	A,
         I--J	O,
         I--K	A,
         I--O	A,
         K--L	D,
         K--O	A,
         L--M	E,F,

         */
    }
}
