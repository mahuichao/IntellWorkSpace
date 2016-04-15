package com.mhc.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * 主要起到主动把maptask的分区指定为我们指定的个数
 * 主要为了方便业务逻辑操作
 */
public class ProvincePartition extends Partitioner<Text, FlowBean> {
    private static Map<String, Integer> proviceDict = new HashMap<String, Integer>();

    static {
        /**
         * 我们假定这些编号对应着不同的省份
         */
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
