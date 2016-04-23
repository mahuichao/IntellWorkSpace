package com.pv;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2016/4/23.
 */
public class PageViewsMapper extends Mapper<LongWritable, Text, Text, PvLoggers> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] cont = line.split("\t");
        String ip = cont[0];
        String date = cont[1];
        String session = cont[2];
        String url = cont[3];
        String referal = cont[4];
        PvLoggers pvLoggers = new PvLoggers();
        pvLoggers.set(ip, session, date, url, "default", "default"); // 还没有进行时间筛选，所以有些变量默认初始化值为default
        context.write(new Text(ip), pvLoggers);
    }
}
