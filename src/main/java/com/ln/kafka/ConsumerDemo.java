package com.ln.kafka;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerDemo {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop1:9092");
        //消费者组id
        properties.put("group.id", "group-1");
        //设置自动提交
        properties.put("enable.auto.commit", "true");
        //延迟提交
        properties.put("auto.commit.interval.ms", "1000");
        //设置offset  表示将offset置到哪里
        properties.put("auto.offset.reset", "earliest");
        //休息时间
        properties.put("session.timeout.ms", "30000");
        //kv反序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建kafka消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        //topic
        kafkaConsumer.subscribe(Arrays.asList("bawei"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);

            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }
}
