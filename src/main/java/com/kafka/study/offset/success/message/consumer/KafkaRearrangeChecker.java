package com.kafka.study.offset.success.message.consumer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaRearrangeChecker {
    private static Logger logger = LoggerFactory.getLogger(KafkaRearrangeChecker.class);
    public static void main(String[] args) {

        KafkaRearrangeChecker commitConsumer = new KafkaRearrangeChecker();
        logger.error("error test");
        commitConsumer.checkTimestampOrdered();
    }

    public void checkTimestampOrdered(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.90.153:9092");
        props.put("group.id", "group-aaa");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("auto.offset.reset", "earliest");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("auto.commit.interval.ms", 1000*600);
        props.put("max.poll.interval.ms", 1000*60*5);

        props.put("max.poll.records", 100);
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                        + "admin" + "\" password=\"" + "admin" + "\";");
        @SuppressWarnings("resource")
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("bingoyes-imsi-rearranged22"));

        List<ConsumerRecord<String, String>> list = new ArrayList<>();

        long lastTimstamp = 0;
        long total = 0;
        long outOfOrderTotal = 0;
        long boundaryMilestone = -1;
        long boundaryStart = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(6000));
            for (ConsumerRecord<String, String> record : records) {
                //logger.info(String.format("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value()));
                JSONObject recordJson = JSON.parseObject(record.value());

                long curTimestamp = recordJson.getLong("timestamp");
                logger.info("read one record, timestamp:"+curTimestamp);
                if(boundaryMilestone==-1 && curTimestamp<lastTimstamp){

                    boundaryMilestone = lastTimstamp;
                    boundaryStart = curTimestamp;
                    logger.error("enter boundary,start:"+curTimestamp);
                    logger.error("boundary previous timestamp:"+lastTimstamp);
                    logger.error("current total:"+total);

                    logger.error("current offset:"+record.offset()+",value="+record.value());


                }else if(boundaryMilestone>0 && curTimestamp<boundaryMilestone){
                    logger.error("one outof order record:"+curTimestamp);
                    outOfOrderTotal++;
                }else if(boundaryMilestone>0 && curTimestamp>=boundaryMilestone) {
                    logger.error("end of boundary,start:"+boundaryStart+",end:" + curTimestamp+",boundaryMilestone:"+boundaryMilestone);
                    logger.error("total out of order :"+outOfOrderTotal);
                    outOfOrderTotal=0;
                    boundaryMilestone=-1;
                    boundaryStart = 0;
                }
                lastTimstamp = curTimestamp;
                total++;
            }
            //consumer.commitSync();

        }
    }

}