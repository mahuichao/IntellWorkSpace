package com.log.enhance;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Administrator on 2016/4/16.
 */
public class LogEnhance {
    static class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        HashMap<String, String> myMap = new HashMap<String, String>();
        Text k = new Text();
        NullWritable v = NullWritable.get();

        /**
         * 在运行之前把myMap装载完毕
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                DataLoader.getDataFromDB(myMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Counter count = context.getCounter("malformed", "malformedline"); //计数器 用来记录不合法的日志行数
            String res = value.toString();
            String[] values = StringUtils.split(res, "\t");
            try {
                String http = values[26];
                String content = myMap.get(http);
                if (content == null) { // 为空，证明我们的基础库中并没有此网站信息，写往toCrawl
                    k.set(http + "toCrawl");
                    context.write(k, v);
                } else {
                    k.set(res + "\t" + content + "\n");
                    context.write(k, v);
                }
            } catch (Exception e) {
                count.increment(1);
            }

        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);
            job.setMapperClass(LogEnhanceMapper.class);


            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setOutputFormatClass(LogEnhanceOutputFormat.class);


            FileInputFormat.setInputPaths(job, new Path("file:///D:/input")); // 我把我的文件放在了这个下面
            Path outPath = new Path("file:///D:/output"); // 输出路径在这下面
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);
            job.setNumReduceTasks(0);
            boolean bl = job.waitForCompletion(true);
            System.exit(bl ? 0 : 1);
        }
    }
}
