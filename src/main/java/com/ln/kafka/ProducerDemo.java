package com.ln.kafka;

import java.util.Properties;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka集群
        properties.put("bootstrap.servers", "hadoop1:9092");
        //应答级别
        properties.put("acks", "all");
        //重试次数
        properties.put("retries",0);
        //批次大小
        properties.put("batch.size", 16384);
        //延迟提交
        properties.put("linger.ms", 1);
        //缓存
        properties.put("buffer.memory", 33554432);
        //kv 序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = null;

        try {
            //创建kafka生产者
            producer = new KafkaProducer<String, String>(properties);
            for (int i = 0; i < 10; i++) {
                String msg = "------Message " + i;
                //循环发送消息
                producer.send(new ProducerRecord<String, String>("bawei", msg));
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            //关闭资源
            producer.close();
        }


    }
}

