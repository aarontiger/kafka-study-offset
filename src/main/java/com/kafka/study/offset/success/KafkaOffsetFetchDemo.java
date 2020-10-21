package com.kafka.study.offset.success;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.study.offset.test.zkapi.MyZkWatcher;
import com.kafka.study.offset.success.model.OffsetInfo;
import com.kafka.study.offset.success.model.OffsetZkInfo;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KafkaOffsetFetchDemo {

    private final String BROKER_TOPICS_PATH = "/brokers/topics";
    private final String KAFKA_EAGLE_SYSTEM_GROUP = "kafka.eagle.system.group";


 /*   private final String KAFKA_SERVER_URL ="192.168.66.121:9092";
    private final String ZOOKEEPER_SERVER_URL ="192.168.66.121:2181";
    public final String KAFKA_TOPIC = "powerTopic";
    public final String KAFKA_GROUP = "lovelyGroup";*/

    public static final String KAFKA_SERVER_URL ="192.168.90.5:9092";
    private final String ZOOKEEPER_SERVER_URL ="192.168.90.5:9081";
    public final String KAFKA_TOPIC = "bingoyes-face";
    public final String KAFKA_GROUP = "flink-graph-face4";

    KafkaServiceImpl kafkaService = new KafkaServiceImpl();

    AdminClient adminClient;

    public KafkaOffsetFetchDemo(){


        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);

        prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        prop.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        prop.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        "admin\" password=\"admin\";");
        adminClient = AdminClient.create(prop);
    }

    public static void main(String[] args){
        KafkaOffsetFetchDemo kafkaOffsetFetchDemo = new KafkaOffsetFetchDemo();
        kafkaOffsetFetchDemo.getKafkaOffSet(kafkaOffsetFetchDemo.KAFKA_TOPIC,kafkaOffsetFetchDemo.KAFKA_GROUP);
    }

    public void getKafkaOffSet(String topic,String group){
        Set<Integer> partitionids = this.getTopicPartitions(topic);

     /*   AdminClient adminClient = null;
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER_URL);

        prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        prop.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        prop.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        "admin\" password=\"admin\";");*/

        try {
            //adminClient = AdminClient.create(prop);
            List<TopicPartition> tps = new ArrayList<>();
            for (int partitionid : partitionids) {
                TopicPartition tp = new TopicPartition(topic, partitionid);
                tps.add(tp);
            }

            ListConsumerGroupOffsetsOptions consumerOffsetOptions = new ListConsumerGroupOffsetsOptions();
            consumerOffsetOptions.topicPartitions(tps);

            ListConsumerGroupOffsetsResult offsets = adminClient.listConsumerGroupOffsets(group);
            Map<Integer, Long> partitionOffset = new HashMap<>();

            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.partitionsToOffsetAndMetadata().get().entrySet()) {
                if (topic.equals(entry.getKey().topic())) {
                    partitionOffset.put(entry.getKey().partition(), entry.getValue().offset());
                }
            }

            System.out.println("partitionOffeset:"+partitionOffset);

            Map<TopicPartition, Long> tps2 = getKafkaLogSize(KAFKA_SERVER_URL, topic, partitionids);
            List<OffsetInfo> resultOffsetList = new ArrayList<OffsetInfo>();
            if (tps != null && partitionOffset != null) {
                for (Map.Entry<TopicPartition, Long> entrySet : tps2.entrySet()) {
                    OffsetInfo offsetInfo = new OffsetInfo();
                    int partition = entrySet.getKey().partition();
                    offsetInfo.setCreate("2020-10-20");
                    offsetInfo.setModify("2020-10-20");
                    offsetInfo.setLogSize(entrySet.getValue());
                    offsetInfo.setOffset(partitionOffset.get(partition));
                    offsetInfo.setLag(offsetInfo.getOffset() == -1 ? 0 : (offsetInfo.getLogSize() - offsetInfo.getOffset()));
                    offsetInfo.setOwner(getKafkaOffsetOwner( group, topic, partition).getOwners());
                    offsetInfo.setPartition(partition);
                    resultOffsetList.add(offsetInfo);
                    System.out.println("offset item:"+offsetInfo);
                }
            }
            System.out.println("result offset list:"+resultOffsetList);
            //return targets;


        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 得到topic的分区信息
     * @param topic
     * @return
     */
    public Set<Integer> getTopicPartitions(String topic){
        String ZKServers = ZOOKEEPER_SERVER_URL;
        ZkClient zkClient = new ZkClient(ZKServers,10000,60000,new SerializableSerializer());
        System.out.println("conneted ok!");

        List<String> brokerTopicsPaths = zkClient.getChildren(BROKER_TOPICS_PATH + "/" + topic + "/partitions");
        Set<Integer> partitionIds = new HashSet<Integer>();
        for(String entry:brokerTopicsPaths)
        {
            partitionIds.add(Integer.parseInt(entry));
        }

        return partitionIds;
    }

    //得到owner
    public OffsetZkInfo getKafkaOffsetOwner(String group, String topic, int partition) {
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

    //得到topic生产的最后位置
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

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        "admin\" password=\"admin\";");

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


    /*public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(props);
        ConsumerGroupSummary  consumerGroupSummary =  adminClient.describeConsumerGroup("kafkatest");
        if(consumerGroupSummary.state().equals("Empty")){
            System.out.println("niaho");
        }
        Option<List<ConsumerSummary>> consumerSummaryOption =  consumerGroupSummary.consumers();

        List<ConsumerSummary> ConsumerSummarys = consumerSummaryOption.get();//获取组中的消费者
        KafkaConsumer consumer = getNewConsumer();
        for(int i=0;i<ConsumerSummarys.size();i++){ //循环组中的每一个消费者

            ConsumerSummary consumerSummary = ConsumerSummarys.apply(i);
            String consumerId  = consumerSummary.consumerId();//获取消费者的id
            scala.collection.immutable.Map<TopicPartition, Object> maps =
                    adminClient.listGroupOffsets("kafkatest");//或者这个组消费的所有topic，partition和当前消费到的offset
            List<TopicPartition> topicPartitions= consumerSummary.assignment();//获取这个消费者下面的所有topic和partion
            for(int j =0;j< topicPartitions.size();j++){ //循环获取每一个topic和partion
                TopicPartition topicPartition = topicPartitions.apply(j);
                String CURRENToFFSET = maps.get(topicPartition).get().toString();
                long endOffset =getLogEndOffset(topicPartition);
                System.out.println("topic的名字为："+topicPartition.topic()+"====分区为："+topicPartition.partition()+"===目前消费offset为："+CURRENToFFSET+"===,此分区最后offset为："+endOffset);
            }
        }
    }

    public static KafkaConsumer getNewConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "kafkatest");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public static long getLogEndOffset(TopicPartition topicPartition){
        KafkaConsumer<String, String> consumer= getNewConsumer();
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));
        long endOffset = consumer.position(topicPartition);
        return endOffset;
    }*/



}
