package com.kafka.study.offset.test.zkapi;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestOnZkAuth {
    private final String ZOOKEEPER_SERVER_URL ="192.168.90.5:9081";
    private static final String allAuth = "super-admin:admin1";
    private static final String digest = "digest";
    private static final String testNode = "/dubbo";

    public void testZkAuth(){
        String ZKServers = ZOOKEEPER_SERVER_URL;
        ZkClient zkClient = new ZkClient(ZKServers,10000,60000,new SerializableSerializer());
        try {
            System.out.println(DigestAuthenticationProvider.generateDigest(allAuth));
            zkClient.addAuthInfo(digest, allAuth.getBytes());
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        if (zkClient.exists(testNode)) {
            zkClient.delete(testNode);
            System.out.println("节点删除成功！");
        }
    }

    public ZooKeeper connect() throws IOException, InterruptedException{
        CountDownLatch cdl = new CountDownLatch(1);
//        log.info("准备建立zk服务");
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_SERVER_URL, 100000,new MyZkWatcher(cdl,"建立连接"));
//        log.info("完成建立zk服务");
        cdl.await();//这里为了等待wather监听事件结束
        //zk.addAuthInfo("digest", allAuth.getBytes());

        try {
            Stat stat = queryStat(zk, "/");
            System.out.println("state:"+stat);
        } catch (KeeperException e) {
            e.printStackTrace();
        }
        List<ACL> acls = new ArrayList<>();
        //scheme 有world/auth/digest/host/ip/
        //zk的digest是通过sha1加密
        String scheme = "auth";
        //定义一个用户名密码为lry:123456
        Id id = null;
        try {
            id = new Id(scheme, DigestAuthenticationProvider.generateDigest("admin:admin"));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        ACL acl = new ACL(ZooDefs.Perms.ALL, id);
        acls.add(acl);
        return zk;
    }

    public static Stat queryStat(ZooKeeper zk,String nodePath) throws KeeperException, InterruptedException{
        //log.info("准备查询节点Stat，path：{}", nodePath);
        Stat stat = zk.exists(nodePath, false);
        //log.info("结束查询节点Stat，path：{}，version：{}", nodePath, stat.getVersion());
        return stat;
    }

}
