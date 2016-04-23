package com.index;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by Administrator on 2016/4/21.
 */
public class LogReducer extends Reducer<Text, Loggers, Text, Loggers> {
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

        Collections.sort(list, new Comparator<Loggers>() {
            public int compare(Loggers o1, Loggers o2) {
                if (getTimeStamp(o1.getDate()) > getTimeStamp(o2.getDate())) {
                    return 1;
                } else if (getTimeStamp(o1.getDate()) < getTimeStamp(o2.getDate())) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });


        for (int i = 0; i < list.size(); i++) {
            int s = list.size();
            String session = UUID.randomUUID().toString();
            list.get(i).setSession(session);
            context.write(key, list.get(i));
        }
    }

    public long getTimeStamp(String myDate) {
        String s = myDate;
        long cur = Timestamp.valueOf(myDate).getTime();
        return cur;
    }
}
