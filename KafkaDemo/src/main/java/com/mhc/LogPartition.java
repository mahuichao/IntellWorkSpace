package com.mhc;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
import org.apache.log4j.Logger;


/**
 * 自定义分组策略
 * Created by Administrator on 2016/5/5.
 */
public class LogPartition implements Partitioner {
    public static Logger logger = Logger.getLogger(LogPartition.class);

    public LogPartition(VerifiableProperties props) {
    }

    public int partition(Object key, int numPartitions) {
        return Integer.parseInt(key.toString()) % numPartitions;
    }
}
