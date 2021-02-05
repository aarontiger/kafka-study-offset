package com.kafka.study.offset.success.message.consumer;

import java.time.Duration;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaManualCommitConsumer {
    private static Logger logger = LoggerFactory.getLogger(KafkaManualCommitConsumer.class);
    public static void main(String[] args) {

        KafkaManualCommitConsumer commitConsumer = new KafkaManualCommitConsumer();
        logger.error("error test");
        commitConsumer.checkTimestampOrdered();

    }

    public void checkMessageCount(){
        Properties props = new Properties();
        //props.put("bootstrap.servers", "103.231.146.62:19092");
        props.put("bootstrap.servers", "192.168.60.204:9092");
        props.put("group.id", "group-haoshuhu123");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("auto.commit.interval.ms", 1000*600);
        props.put("max.poll.interval.ms", 1000*60*5);
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + "admin" + "\" password=\"" + "admin" + "\";");
        @SuppressWarnings("resource")
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("bingoyes-imsi-rearranged"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> list = new ArrayList<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                list.add(record);
                Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(10757388, "no metadata"));

                //每2次提交一次，还可以根据时间间隔来提交
                consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (null != exception){
                            System.out.println(String.format("==== Commit failed for offsets %s, error:%s ====", offsets, exception.toString()));
                        }
                    }
                });
                break;

            }
            //consumer.commitSync();


            /*if (list.size() >= minBatchSize) {
                System.out.println("list中的缓存数据大于minBatchSize时批量进行处理");
                //todo test code
                //consumer.commitSync();
                System.out.println("全部数据处理成功后手动提交");
                list.clear();
            }*/
        }
    }

    public void checkTimestampOrdered(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("group.id", "group-aaa");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("auto.commit.interval.ms", 1000*600);
        props.put("max.poll.interval.ms", 1000*60*5);
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + "admin" + "\" password=\"" + "admin" + "\";");
        @SuppressWarnings("resource")
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("bingoyes-imsi-rearranged"));

        List<ConsumerRecord<String, String>> list = new ArrayList<>();

        long lastTimstamp = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(6000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                JSONObject recordJson = JSON.parseObject(record.value());

                long curTimestamp = recordJson.getLong("timestamp");

                if(curTimestamp<lastTimstamp){
                    logger.error("order problem,offset:"+record.offset()+",value="+record.value());
                }
            }
            //consumer.commitSync();

        }
    }

}