package com.mhc.stormdemo;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator on 2016/4/27.
 */
public class MySpout extends BaseRichSpout {
    SpoutOutputCollector collector;

    /**
     * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处
     * 该declare变量有很大作用，我们还可以调用declarer.declareStream();来定义streaamId,该
     * Id可以用来定义更加复杂的流拓扑结构
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("love"));
    }

    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        collector.emit(new Values("i am lilei love hanmeimei"));
    }
}
