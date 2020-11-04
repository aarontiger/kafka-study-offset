package com.kafka.study.offset.success.apidemo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.kafka.study.offset.success.apidemo.KafkaOffsetFetchDemo;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaServiceImpl {

    private AdminClient adminClient;

    public KafkaServiceImpl(){
        Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaOffsetFetchDemo.KAFKA_SERVER_URL);

        prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        prop.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        prop.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        "admin\" password=\"admin\";");
        adminClient = AdminClient.create(prop);
    }


    private JSONArray getKafkaMetadata(String bootstrapServers, String group) {
     /*   Properties prop = new Properties();
        prop.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

      *//*  if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.sasl.enable")) {
            sasl(prop, clusterAlias);
        }
        if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.ssl.enable")) {
            ssl(prop, clusterAlias);
        }*//*

        prop.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        prop.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        prop.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" +
                        "admin\" password=\"admin\";");

        AdminClient adminClient = null;*/

        JSONArray consumerGroups = new JSONArray();
        try {
            //adminClient = AdminClient.create(prop);
            DescribeConsumerGroupsResult descConsumerGroup = adminClient.describeConsumerGroups(Arrays.asList(group));
            Collection<MemberDescription> consumerMetaInfos = descConsumerGroup.describedGroups().get(group).get().members();
            Set<String> hasOwnerTopics = new HashSet<>();
            if (consumerMetaInfos.size() > 0) {
                for (MemberDescription consumerMetaInfo : consumerMetaInfos) {
                    JSONObject topicSub = new JSONObject();
                    JSONArray topicSubs = new JSONArray();
                    for (TopicPartition topic : consumerMetaInfo.assignment().topicPartitions()) {
                        JSONObject object = new JSONObject();
                        object.put("topic", topic.topic());
                        object.put("partition", topic.partition());
                        topicSubs.add(object);
                        hasOwnerTopics.add(topic.topic());
                    }
                    topicSub.put("owner", consumerMetaInfo.consumerId());
                    topicSub.put("node", consumerMetaInfo.host().replaceAll("/", ""));
                    topicSub.put("topicSub", topicSubs);
                    consumerGroups.add(topicSub);
                }
            }

            ListConsumerGroupOffsetsResult noActiveTopic = adminClient.listConsumerGroupOffsets(group);
            JSONObject topicSub = new JSONObject();
            JSONArray topicSubs = new JSONArray();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : noActiveTopic.partitionsToOffsetAndMetadata().get().entrySet()) {
                JSONObject object = new JSONObject();
                object.put("topic", entry.getKey().topic());
                object.put("partition", entry.getKey().partition());
                if (!hasOwnerTopics.contains(entry.getKey().topic())) {
                    topicSubs.add(object);
                }
            }
            topicSub.put("owner", "");
            topicSub.put("node", "-");
            topicSub.put("topicSub", topicSubs);
            consumerGroups.add(topicSub);
        } catch (Exception e) {
            //LOG.error("Get kafka consumer metadata has error, msg is " + e.getMessage());
            e.printStackTrace();
        } finally {
            adminClient.close();
        }
        return consumerGroups;
    }

    public String getKafkaConsumerGroupTopic( String group) {
        return getKafkaMetadata(KafkaOffsetFetchDemo.KAFKA_SERVER_URL, group).toJSONString();
    }

    public  ArrayList<String> getAllGroupList(){

        ArrayList<String> retGroupList = new ArrayList<String>();
        ListConsumerGroupsResult result = adminClient.listConsumerGroups();
        KafkaFuture<Collection<ConsumerGroupListing>> future = result.all();

        try {
            Collection<ConsumerGroupListing> groupList =future.get();

            for(ConsumerGroupListing group:groupList){
                retGroupList.add(group.groupId());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        return retGroupList;
    }

   /* private String parseBrokerServer(String clusterAlias) {
        String brokerServer = "";
        List<BrokersInfo> brokers = getAllBrokersInfo(clusterAlias);
        for (BrokersInfo broker : brokers) {
            brokerServer += broker.getHost() + ":" + broker.getPort() + ",";
        }
        if ("".equals(brokerServer)) {
            return "";
        }
        return brokerServer.substring(0, brokerServer.length() - 1);
    }*/

    /**
     * Get all broker list from zookeeper.
     */
 /*   public List<BrokersInfo> getAllBrokersInfo(String clusterAlias) {
        KafkaZkClient zkc = kafkaZKPool.getZkClient(clusterAlias);
        List<BrokersInfo> targets = new ArrayList<BrokersInfo>();
        if (zkc.pathExists(BROKER_IDS_PATH)) {
            Seq<String> subBrokerIdsPaths = zkc.getChildren(BROKER_IDS_PATH);
            List<String> brokerIdss = JavaConversions.seqAsJavaList(subBrokerIdsPaths);
            int id = 0;
            for (String ids : brokerIdss) {
                try {
                    Tuple2<Option<byte[]>, Stat> tuple = zkc.getDataAndStat(BROKER_IDS_PATH + "/" + ids);
                    BrokersInfo broker = new BrokersInfo();
                    broker.setCreated(CalendarUtils.convertUnixTime2Date(tuple._2.getCtime()));
                    broker.setModify(CalendarUtils.convertUnixTime2Date(tuple._2.getMtime()));
                    String tupleString = new String(tuple._1.get());
                    if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.sasl.enable") || SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.ssl.enable")) {
                        String endpoints = JSON.parseObject(tupleString).getString("endpoints");
                        List<String> endpointsList = JSON.parseArray(endpoints, String.class);
                        String host = "";
                        int port = 0;
                        if (endpointsList.size() > 1) {
                            String protocol = "";
                            if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.sasl.enable")) {
                                protocol = Kafka.SASL_PLAINTEXT;
                            }
                            if (SystemConfigUtils.getBooleanProperty(clusterAlias + ".kafka.eagle.ssl.enable")) {
                                protocol = Kafka.SSL;
                            }
                            for (String endpointsStr : endpointsList) {
                                if (endpointsStr.contains(protocol)) {
                                    String tmp = endpointsStr.split("//")[1];
                                    host = tmp.split(":")[0];
                                    port = Integer.parseInt(tmp.split(":")[1]);
                                    break;
                                }
                            }
                        } else {
                            if (endpointsList.size() > 0) {
                                String tmp = endpointsList.get(0).split("//")[1];
                                host = tmp.split(":")[0];
                                port = Integer.parseInt(tmp.split(":")[1]);
                            }
                        }
                        broker.setHost(host);
                        broker.setPort(port);
                    } else {
                        String host = JSON.parseObject(tupleString).getString("host");
                        int port = JSON.parseObject(tupleString).getInteger("port");
                        broker.setHost(host);
                        broker.setPort(port);
                    }
                    broker.setJmxPort(JSON.parseObject(tupleString).getInteger("jmx_port"));
                    broker.setId(++id);
                    broker.setIds(ids);
                    targets.add(broker);
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                }
            }
        }
        if (zkc != null) {
            kafkaZKPool.release(clusterAlias, zkc);
            zkc = null;
        }
        return targets;
    }*/
}
