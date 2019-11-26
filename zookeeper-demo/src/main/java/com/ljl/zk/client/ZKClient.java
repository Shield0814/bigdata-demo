package com.ljl.zk.client;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 添加 digest 认证用户及密码：
 * addauth digest sysadm:oliver8023
 */
public class ZKClient {

    private ZooKeeper zkClient;

    @Before
    public void connect() throws IOException {
        String connetStr = "bigdata116:2181,bigdata117:2181,bigdata117:2181";
        zkClient = new ZooKeeper(connetStr, 1000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("获取客户端成功");
                System.out.println("stat:" + event.getState());
                System.out.println("eventType:" + event.getType());
            }
        });
    }

    @After
    public void close() throws InterruptedException {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    @Test
    public void testSetData() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/test/auth_ip_acl", false);
        zkClient.setData("/test/auth_ip_acl", "test IP acl 认证".getBytes(), stat.getVersion());
    }

    @Test
    public void testDelete() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/test/auth_ip_acl", false);
        zkClient.delete("/test/auth_ip_acl", stat.getVersion());
    }

    @Test
    public void testWatch() throws KeeperException, InterruptedException {
        testLs();
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void testLs() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/test", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("/test 子节点发生了改变：" + event.getType());
                try {
                    testLs();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        children.forEach(child -> {
            System.out.println(child);
        });

        System.out.println("++++++++++++++++++++++++++++++++");
    }

    @Test
    public void exists() throws KeeperException, InterruptedException {
        Stat stat = zkClient.exists("/test", false);
        if (stat != null) {
            System.out.println("czxid:" + stat.getCzxid());
            System.out.println("ctime:" + stat.getCtime());
            System.out.println("cversion:" + stat.getCversion());
            System.out.println("mzxid:" + stat.getMzxid());
            System.out.println("mtime:" + stat.getMtime());
            System.out.println("dataversion:" + stat.getVersion());
            System.out.println("pzxid:" + stat.getPzxid());
            System.out.println("dataLength:" + stat.getDataLength());
            System.out.println("ephemeralOvner:" + stat.getEphemeralOwner());
            System.out.println("aversion:" + stat.getAversion());
            System.out.println("numChildren:" + stat.getNumChildren());
        }
    }

    /**
     * zookeeper digest acl测试
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testCreateNode() throws KeeperException, InterruptedException {
        //通过digest进行认证
        List<ACL> acls1 = new ArrayList<>();
        Stat stat1 = zkClient.exists("/test/auth_digest_acl", null);
        if (stat1 == null) {
            acls1.add(new ACL(ZooDefs.Perms.ALL, new Id("digest", "sysadm:oliver8023")));
            String res1 = zkClient.create("/test/auth_digest_acl",
                    "测试用户名密码认证".getBytes(),
                    acls1,
                    CreateMode.PERSISTENT);
            System.out.println(res1);
        }

        //通过ip进行认证
        Stat stat2 = zkClient.exists("/test/auth_ip_acl", null);
        if (stat2 == null) {
            List<ACL> acls2 = new ArrayList<>();
            acls2.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "127.0.0.1")));
            acls2.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.1")));
            acls2.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.116")));
            String res2 = zkClient.create("/test/auth_ip_acl",
                    "测试IP认证".getBytes(),
                    acls2,
                    CreateMode.PERSISTENT);
            System.out.println(res2);
        }
        //不认证，开方，所有客户端可用
        Stat stat3 = zkClient.exists("/test/auth_ip_acl", null);
        if (stat3 == null) {
            List<ACL> acls3 = new ArrayList<>();
            acls3.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "127.0.0.1")));
            acls3.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.1")));
//           ZooDefs.Ids.OPEN_ACL_UNSAFE 相当于 new ACL(Perms.ALL, ANYONE_ID_UNSAFE)
//           ZooDefs.Ids.ANYONE_ID_UNSAFE 相当于 new Id("world", "anyone");
//           ZooDefs.Ids.CREATOR_ALL_ACL 相当于 new ACL(Perms.ALL, AUTH_IDS)
            String res3 = zkClient.create("/test/auth_ip_acl",
                    "测试IP认证".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            System.out.println(res3);

        }
    }

    /**
     * 测试ip acl
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void testIpAuth() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/test/auth_ip_acl", false, null);
        System.out.println(new String(data));
        List<ACL> acls = new ArrayList<>();
        acls.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.117")));
        acls.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.1")));
        acls.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.116")));

        Stat stat = zkClient.setACL("/test/auth_ip_acl", acls, 1);
    }

    @Test
    public void testDigestAuth() throws KeeperException, InterruptedException {
        zkClient.addAuthInfo("digest", "sysadm:oliver8023".getBytes());
        byte[] data = zkClient.getData("/testacl", false, null);
        System.out.println(new String(data));
    }
}
