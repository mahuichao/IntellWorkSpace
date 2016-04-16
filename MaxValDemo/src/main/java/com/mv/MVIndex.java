package com.mv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by Administrator on 2016/4/16.
 */
public class MVIndex {
    static class MVIndexMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String res = value.toString();
            String[] field = res.split(",");
            OrderBean ob = new OrderBean(new Text(field[0]), new DoubleWritable(Double.parseDouble(field[2])));
            context.write(ob, NullWritable.get());
        }
    }

    static class MVIndexReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        // 到达reduce时，相同id的所有bean已经被看成一组，且金额最大的那个排在第一位
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setMapperClass(MVIndexMapper.class);
        job.setReducerClass(MVIndexReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        /**
         * 我们的orders文件放在D盘的input下面
         */
        FileInputFormat.setInputPaths(job, new Path("D:/input/"));
        FileSystem file = FileSystem.get(conf);
        Path outPath = new Path("D:/output");
        if (file.exists(outPath)) {
            // 如果目标下存在该文件夹，就将其删除。
            file.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 在此设置GroupingCOmparator类
        job.setGroupingComparatorClass(MaxValue.class);
        // 在此设置自定义的partitioner
        job.setPartitionerClass(MaxValPartitioner.class);

        job.setNumReduceTasks(3);
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
        /**
         * 结果各个文件内容如下：
         * Order_0000003	322.8
         * Order_0000001	222.8
         * Order_0000002	522.8
         */
    }
}
