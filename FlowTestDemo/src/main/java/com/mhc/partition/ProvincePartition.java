package com.mhc.partition;

import com.mhc.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * V1 V2 对应map输出的kv类型
 * Created by Administrator on 2016/4/14.
 */
public class ProvincePartition extends Partitioner<Text, FlowBean> {
    private static Map<String, Integer> proviceDict = new HashMap<String, Integer>();

    static {
        proviceDict.put("136", 0);
        proviceDict.put("137", 1);
        proviceDict.put("138", 2);
        proviceDict.put("139", 3);
    }


    public int getPartition(Text key, FlowBean flowBean, int i) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);

        return provinceId == null ? 4 : provinceId;

    }
}
