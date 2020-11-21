package com.kafka.study.offset.success.message.consumer;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class KafkaMessageTotal {

    private static Logger logger = LoggerFactory.getLogger(KafkaMessageTotal.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        //props.put("bootstrap.servers", "103.231.146.62:19092");
        props.put("bootstrap.servers", "192.168.90.5:9092");
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
        String topic = System.getProperty("topic");
        consumer.subscribe(Arrays.asList(topic));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> list = new ArrayList<>();
        long startTime = 0;
        int timestamp =0;
        int sum = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                list.add(record);

                JSONObject jsonObject = JSONObject.parseObject(record.value());
                int timstamp2 = jsonObject.getInteger("timestamp");

                /*if(timstamp2 == timestamp)
                {
                    count++;
                }else{
                    System.out.println("timestamp:"+timestamp);
                    System.out.println("sum:"+count);
                    count = 0;
                    timestamp = timstamp2;
                }*/

                long currentTime = new Date().getTime()/1000;
                if(startTime == 0) {
                    startTime = currentTime;
                    continue;
                }


                if(currentTime-startTime>=10)
                {

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    logger.info("topic:"+topic+",time:"+sdf.format(new Date(startTime*1000))+",sum:"+sum);
                    startTime = currentTime;
                    sum = 0;
                }else
                {
                    sum ++;
                }
            }

        }

    }

}