package com.wzl.kafka.synchronization;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 不带回调函数的API
 */
public class ProducerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
//        properties.put("bootstrap.servers", "hadoop1:9092");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop1:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
//        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<String, String> producer = null;

        try {
            //同步发送消息
            producer = new KafkaProducer<String, String>(properties);
            Future<RecordMetadata> send = producer.send(new ProducerRecord<String, String>("hello_topic", "这是一条秘闻"));
            RecordMetadata recordMetadata = send.get();
            System.out.println("Topic:"+recordMetadata.topic()+"\n"+
                                "partition:"+recordMetadata.partition()+"\n"+
                                "offset:"+recordMetadata.offset());
            //TODO  异步发送消息
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
