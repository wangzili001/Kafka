package com.wzl.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {
        //配置信息
        Properties props = new Properties();
        //Kafka集群
        props.put("bootstrap.servers", "master:9092");
        //ack应答级别
        props.put("acks","all");
        //重试次数
        props.put("retries",0);
        //批量大小
        props.put("batch.size",16384);
        //提交延迟
        props.put("linger.ms",1);
        //缓存
        props.put("buffer.memory",33554432);
        //KV序列化类
        props.put("key.serializer", "org.apach.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apach.kafka.common.serialization.StringSerializer");
        //创建生产者对象
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<Object, Object>("first",String.valueOf(i)));
        }
        producer.close();
    }
}
