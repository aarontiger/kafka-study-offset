package com.kafka.study.offset.success;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.study.offset.success.model.OffsetZkInfo;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class OwnerFetcher {

    private final String KAFKA_SERVER_URL ="192.168.66.121:9092";


    KafkaServiceImpl kafkaService = new KafkaServiceImpl();


}
