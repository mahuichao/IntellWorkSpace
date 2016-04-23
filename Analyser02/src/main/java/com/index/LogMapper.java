package com.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2016/4/21.
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, Loggers> {
    Map<String, String> map = new HashMap<String, String>();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String session = "default";
            String line = value.toString(); // 拿到字符串
            map = getEachPart(line);
            Loggers log = new Loggers();
            log.set(map.get("date"), map.get("ip"), map.get("url"), map.get("referal"), session);
            context.write(new Text(log.getIp()), log);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private Map<String, String> getEachPart(String line) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", Locale.ENGLISH);
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Map<String, String> result = new HashMap<String, String>();
        //正则表达式
        String regex = "(\\d+\\.\\d+\\.\\d+\\.\\d+).*\\[(.*?)\\]\\s+\"(.*?)\".*?\\s\"(.*?)\"\\s"; //(\d+\.\d+\.\d+\.\d+).*\[(.*?)\]\s+.*?\s(.*?)\s.*?\s"(.*?)"\s
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            String tmp = matcher.group(2);
            Date date = format.parse(matcher.group(2));
            String specDate = format2.format(date);
            result.put("ip", matcher.group(1));
            result.put("date", specDate);
            result.put("url", (matcher.group(3).equals("-")||matcher.group(3).equals("")) ? "-" : matcher.group(3).split(" ")[1]);
            result.put("referal", matcher.group(4));
        }
        return result;
    }
}
