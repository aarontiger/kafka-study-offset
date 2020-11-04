package com.kafka.study.offset.success.message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;

public class KafkaManualCommitConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "103.231.146.62:39092");
        //props.put("bootstrap.servers", "192.168.90.3:9992");
        props.put("group.id", "report-obuid");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + "admin" + "\" password=\"" + "admin" + "\";");
        @SuppressWarnings("resource")
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("report-obuid"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                list.add(record);
            }
            if (list.size() >= minBatchSize) {
                System.out.println("list中的缓存数据大于minBatchSize时批量进行处理");
                //todo test code
                //consumer.commitSync();
                System.out.println("全部数据处理成功后手动提交");
                list.clear();
            }
        }

    }

}