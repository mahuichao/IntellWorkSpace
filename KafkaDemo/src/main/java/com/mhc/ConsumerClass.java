package com.mhc;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2016/5/5.
 */
public class ConsumerClass implements Runnable {
    public String title;
    public KafkaStream<byte[], byte[]> stream;

    public ConsumerClass(String title, KafkaStream<byte[], byte[]> stream) {
        this.title = title;
        this.stream = stream;
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("group.id", "1");
        properties.put("zookeeper.connect", "moon:2181,jupiter:2181,neptune:2181");
        properties.put("auto.offset.reset", "largest");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("partition.assignment.strategy", "roundrobin");

        ConsumerConfig config = new ConsumerConfig(properties);
        String topic1 = "chaoge";
        String topic2 = "test";
        //只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config);
        //定义一个map
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic1, 3);
        //Map<String, List<KafkaStream<byte[], byte[]>> 中String是topic， List<KafkaStream<byte[], byte[]>是对应的流
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamsMap = consumerConnector.createMessageStreams(topicCountMap);
        //取出kafkaTest对应的streams
        List<KafkaStream<byte[], byte[]>> streams = topicStreamsMap.get(topic1);
        //创建一个容量为4的线程池
        ExecutorService executor = Executors.newFixedThreadPool(3);
        // 创建20个consumer threads
        for (int i = 0; i < streams.size(); i++) {
            executor.execute(new ConsumerClass("消费者" + (i + 1), streams.get(i)));
        }
    }

    public void run() {

        System.out.println("开始运行" + title);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        /**
         * 不停地从stream读取新到来的消息，在等待新的消息时，hasNext()会阻塞
         * 如果调用 `ConsumerConnector#shutdown`，那么`hasNext`会返回false
         * */
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> data = it.next();
            String topic = data.topic();
            int partition = data.partition();
            long offset = data.offset();
            String msg = new String(data.message());
            System.out.println(String.format(
                    "Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
                    title, topic, partition, offset, msg));
        }
        System.out.println(String.format("Consumer: [%s] exiting ...", title));
    }
}
