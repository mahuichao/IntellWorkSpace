package com.pv;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/4/23.
 */
public class PageViewsReducer extends Reducer<Text, PvLoggers, Text, PvLoggers> {


    /**
     * 成功进入此方法的，都是已经排序完毕的
     *
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<PvLoggers> values, Context context) throws IOException, InterruptedException {
        List<PvLoggers> list = new ArrayList<PvLoggers>();
        for (PvLoggers logger : values) {  // 把所有的logger放入list里面
            try {
                PvLoggers loggers = new PvLoggers();
                BeanUtils.copyProperties(loggers, logger);
                list.add(loggers);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        }
        int count = 1;
        long timeDis = 0;
        if (list.size() > 1) {
            for (int i = 0; i < list.size(); i++) { // 默认停留60分钟
                if (i == list.size()-1) {
                    list.get(i).setStayTime("60");
                    list.get(i).setStep(count + "");
                    context.write(key, list.get(i));
                    count++;
                    break;
                }
                if (judgeMethod(list.get(i), list.get(i + 1))) { // 证明不是同一个session
                    list.get(i).setStep(count + "");
                    list.get(i).setStayTime((timeDis == 0 ? 60 : timeDis) + "");
                    context.write(key, list.get(i));
                    count++;
                    continue;
                } else { // 在同一个session
                    timeDis += difference(list.get(i), list.get(i + 1));
                    continue;
                }
            }
        } else { // 如果就一条内容，那么就直接输出
            list.get(0).setStayTime("60");
            list.get(0).setStep(count + "");
            context.write(key, list.get(0));
        }
    }

    public long getTimeStamp(String myDate) {
        String s = myDate;
        long cur = Timestamp.valueOf(myDate).getTime();
        return cur;
    }

    public boolean judgeMethod(PvLoggers p1, PvLoggers p2) {
        long standard = 1000 * 60 * 30;
        String tmp1 = p1.getDate();
        String tmp2 = p2.getDate();
        long pre = getTimeStamp(tmp1);
        long aft = getTimeStamp(tmp2);
        if ((aft - pre) > standard) { // 如果时间差大于半个小时 证明为两个session
            return true;
        } else { // 不在半个小时内
            return false;
        }
    }

    /**
     * 同一次会话的时间差
     *
     * @param p1
     * @param p2
     * @return
     */
    public long difference(PvLoggers p1, PvLoggers p2) {
        String tmp1 = p1.getDate();
        String tmp2 = p2.getDate();
        long pre = getTimeStamp(tmp1);
        long aft = getTimeStamp(tmp2);
        return ((aft - pre) / (1000 * 60));
    }
}
