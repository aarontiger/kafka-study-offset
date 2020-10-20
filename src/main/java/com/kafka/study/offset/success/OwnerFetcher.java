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
    private final String KAFKA_EAGLE_SYSTEM_GROUP = "kafka.eagle.system.group";

    KafkaServiceImpl kafkaService = new KafkaServiceImpl();

    public OffsetZkInfo getKafkaOffsetOwner( String group, String topic, int partition) {
        OffsetZkInfo targetOffset = new OffsetZkInfo();
        JSONArray consumerGroups = JSON.parseArray(kafkaService.getKafkaConsumerGroupTopic( group));
        for (Object consumerObject : consumerGroups) {
            JSONObject consumerGroup = (JSONObject) consumerObject;
            for (Object topicSubObject : consumerGroup.getJSONArray("topicSub")) {
                JSONObject topicSub = (JSONObject) topicSubObject;
                if (topic.equals(topicSub.getString("topic")) && partition == topicSub.getInteger("partition")) {
                    targetOffset.setOwners(consumerGroup.getString("node") + "-" + consumerGroup.getString("owner"));
                }
            }
        }
        return targetOffset;
    }

    public Map<TopicPartition, Long> getKafkaLogSize(String clusterAlias, String topic, Set<Integer> partitionids) {
        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG,KAFKA_EAGLE_SYSTEM_GROUP);
                props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
       /* if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.sasl.enable")) {
            sasl(props, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.ssl.enable")) {
            ssl(props, clusterAlias);
        }*/
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Set<TopicPartition> tps = new HashSet<>();
        for (int partitionid : partitionids) {
            TopicPartition tp = new TopicPartition(topic, partitionid);
            tps.add(tp);
        }

        consumer.assign(tps);
        java.util.Map<TopicPartition, Long> endLogSize = consumer.endOffsets(tps);
        if (consumer != null) {
            consumer.close();
        }
        return endLogSize;
    }
}
