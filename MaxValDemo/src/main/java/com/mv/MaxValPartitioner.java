package com.mv;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by Administrator on 2016/4/16.
 */
public class MaxValPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        // 将相同的bean会发往相同的partition，而且产生的分区数，与用户设置的reduce task数保持一致
        return (orderBean.getItemid().hashCode() & Integer.MAX_VALUE) % i; // 进行模除，对数据进行分区使用
    }
}
