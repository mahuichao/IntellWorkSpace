package com.mhc.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper类，用来对数据进行读取，把同类key放入到一起发送给reduce
 * Created by Administrator on 2016/4/14.
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = value.toString();
        String[] res = s.split("\t");
        String flag = res[1];
        long uploads = Long.parseLong(res[res.length - 3]);
        long dnloads = Long.parseLong(res[res.length - 2]);
        FlowBean fb = new FlowBean(uploads, dnloads);
        context.write(new Text(flag), fb);
    }
}
