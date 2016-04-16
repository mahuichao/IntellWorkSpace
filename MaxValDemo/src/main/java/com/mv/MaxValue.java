package com.mv;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 利用reduce端的GroupingComparator来实现将一组bean看成相同的key
 * Created by Administrator on 2016/4/16.
 */
public class MaxValue extends WritableComparator {
    /**
     * 这个构造函数不能丢，是用来注册我们自己的Bean的
     */
    public MaxValue() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean abean = (OrderBean) a;
        OrderBean bbean = (OrderBean) a;
        // 比较两个bean时，指定比较bean中的orderid
        return abean.getItemid().compareTo(bbean.getItemid());
    }
}
