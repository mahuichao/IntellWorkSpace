package com.mhc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.mhc.com.mhc.bean.Message;
import com.mhc.com.mhc.bean.Record;
import org.apache.log4j.Logger;
import org.springframework.beans.BeanUtils;

/**
 * Created by Administrator on 2016/5/6.
 */
public class PrepareRecordBolt extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(PrepareRecordBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        Message message = (Message) input.getValueByField("message");
        String appId = input.getStringByField("appId");
        MonitorHandler.notifly(appId, message);
        Record record = new Record();
        try {
            BeanUtils.copyProperties(record, message);
            collector.emit(new Values(record));
        } catch (Exception e) {

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }
}
