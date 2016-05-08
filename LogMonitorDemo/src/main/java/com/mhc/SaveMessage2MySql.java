package com.mhc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.mhc.com.mhc.bean.Record;
import org.apache.log4j.Logger;

/**
 * Created by Administrator on 2016/5/6.
 */
public class SaveMessage2MySql extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(SaveMessage2MySql.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        Record record = (Record) input.getValueByField("record");
        MonitorHandler.save(record);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
