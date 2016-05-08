package com.mhc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * 日志监控系统
 * Created by Administrator on 2016/5/6.
 */
public class StartMain {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
//        BrokerHosts hosts = new ZkHosts("moon:2181,jupiter:2181,neptune:2181");
//        SpoutConfig config = new SpoutConfig(hosts, "logmonitor", "/aaa", "log_monitor");
//        topologyBuilder.setSpout("kafka-spout", new KafkaSpout(config));
        topologyBuilder.setSpout("kafka-spout",new RandomSpout(new StringScheme()),2);
        topologyBuilder.setBolt("filter-bolt", new FilterBolt(), 3).shuffleGrouping("kafka-spout");
        topologyBuilder.setBolt("prepareRecord-bolt", new PrepareRecordBolt(), 2).fieldsGrouping("filter-bolt", new Fields("appId"));
        topologyBuilder.setBolt("saveMessage-bolt", new SaveMessage2MySql(), 2).shuffleGrouping("prepareRecord-bolt");

        // 启动topology的配置信息
        Config config1 = new Config();
        //TOPOLOGY_DEBUG(setDebug), 当它被设置成true的话， storm会记录下每个组件所发射的每条消息。
        //这在本地环境调试topology很有用， 但是在线上这么做的话会影响性能的。
        config1.setDebug(true);
        //storm的运行有两种模式: 本地模式和分布式模式.
        if (args != null && args.length > 0) {
            //定义你希望集群分配多少个工作进程给你来执行这个topology
            config1.setNumWorkers(2);
            //向集群提交topology
            StormSubmitter.submitTopologyWithProgressBar(args[0], config1, topologyBuilder.createTopology());
        } else {
            config1.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", config1, topologyBuilder.createTopology());
            Utils.sleep(10000000);
            cluster.shutdown();
        }


    }
}
