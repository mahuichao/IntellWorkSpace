package com.mhc;


import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mhc.com.mhc.bean.Message;
import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2016/5/6.
 */
public class FilterBolt extends BaseBasicBolt {

    private static Logger logger = Logger.getLogger(FilterBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
//        byte[] value = (byte[]) input.getValue(0);
        String line = input.getString(0);
        Message parser = MonitorHandler.parser(line);
        if (parser == null) {
            return;
        }
        // 对信息进行过滤，看是否符合标准
        if (MonitorHandler.trigger(parser)) {
            collector.emit(new Values(parser.getAppId(), parser));
        }
        //定时更新规则信息
        MonitorHandler.scheduleLoad();
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 此处根据appId分组利于以后的区分数据来源
        outputFieldsDeclarer.declare(new Fields("appId", "message"));
    }
}
