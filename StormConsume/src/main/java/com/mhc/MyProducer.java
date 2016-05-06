package com.mhc;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by Administrator on 2016/5/5.
 */
public class MyProducer {
    public static void main(String[] args) throws InterruptedException {
        String TOPIC = "orderMq1";
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "sun:9092,moon:9092,jupiter:9092,neptune:9092");
        props.put("request.required.acks", "1");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));

        for (int messageNo = 1; messageNo < 100000; messageNo++) {
            Thread.sleep(2000);
            System.out.println(messageNo + "---");
            producer.send(new KeyedMessage<String, String>(TOPIC, messageNo + "", new OrderInfo().random()));
        }
    }
}
