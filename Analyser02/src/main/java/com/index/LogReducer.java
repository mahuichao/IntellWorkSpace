package com.index;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Created by Administrator on 2016/4/21.
 */
public class LogReducer extends Reducer<Text, Loggers, Text, Loggers> {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

    @Override
    protected void reduce(Text key, Iterable<Loggers> values, Context context) throws IOException, InterruptedException {
        List<Loggers> list = new ArrayList<Loggers>();
        for (Loggers logger : values) {  // 把所有的logger放入list里面
            try {
                Loggers loggers = new Loggers();
                BeanUtils.copyProperties(loggers, logger);
                list.add(loggers);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < list.size(); i++) {
            int s = list.size();
            String session = UUID.randomUUID().toString();
            if (i == 0) { // 第一次进来
                list.get(0).setSession(session);
                context.write(key, list.get(0));
                continue;
            } else if (i < list.size() - 1) {
                Loggers logger_pre = list.get(i - 1); // 第一次
                Loggers logger_aft = list.get(i); // 下一次
                long standard = 1000 * 60 * 30;
                String tmp = logger_aft.getDate();
                long pre = getTimeStamp(logger_aft.getDate());
                long aft = getTimeStamp(logger_pre.getDate());
                if ((pre - aft) > standard) { // 如果时间差大于半个小时 证明为两个session
                    logger_aft.setSession(session);
                    context.write(key, logger_aft);
                    continue;
                } else { // 不在半个小时内
                    continue;
                }
            } else {
                list.get(i).setSession(session);
                context.write(key, list.get(i));
                continue;
            }
        }
    }


    public long getTimeStamp(String myDate) {
        String s = myDate;
        long cur = Timestamp.valueOf(myDate).getTime();
        return cur;
    }
}
