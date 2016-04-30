package com.mhc.stormdemo;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created by Administrator on 2016/4/27.
 */
public class StormDemo {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new MySpout(), 2);
        topologyBuilder.setBolt("myBolt1", new MyCountBolt(), 2).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("myBolt2", new MyCountBolt2(), 4).fieldsGrouping("myBolt1", new Fields("word")); // 最后的word与MyCountBolt里面declared相对应


        Config conf = new Config();
        conf.setNumWorkers(2);

        //本地运行
        //LocalCluster localCluster = new LocalCluster();
        //localCluster.submitTopology("mywordcount", conf, topologyBuilder.createTopology());

        StormSubmitter.submitTopology("mywordcount", conf, topologyBuilder.createTopology());
    }

}
