package com.mhc.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 对数据进行处理输出结果的Reduce类
 * Created by Administrator on 2016/4/14.
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up_flow = 0;
        long dn_flow = 0;

        for (FlowBean fb : values) {
            up_flow += fb.getUpload();
            dn_flow += fb.getDnload();
        }

        FlowBean flb = new FlowBean(up_flow, dn_flow);
        context.write(key, flb);
    }
}
