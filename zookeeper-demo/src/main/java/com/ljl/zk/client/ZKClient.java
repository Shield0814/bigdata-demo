package com.ljl.zk.client;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ZKClient {

    private ZooKeeper zkClient;

    @Before
    public void connect() throws IOException {
        String connetStr = "bigdata116:2181,bigdata117:2181,bigdata117:2181";
        zkClient = new ZooKeeper(connetStr, 1000, null);
    }

    @After
    public void close() throws InterruptedException {
        if (zkClient != null) {
            zkClient.close();
        }
    }

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

    @Test
    public void testIpAuth() throws KeeperException, InterruptedException {
        byte[] data = zkClient.getData("/testacl1", false, null);
        System.out.println(new String(data));
        List<ACL> acls = new ArrayList<>();
        acls.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.117")));
        acls.add(new ACL(ZooDefs.Perms.ALL, new Id("ip", "192.168.211.1")));
        Stat stat = zkClient.setACL("/testacl1", acls, 1);
    }

    @Test
    public void testDigestAuth() throws KeeperException, InterruptedException {
        zkClient.addAuthInfo("digest", "sysadm:oliver8023".getBytes());
        byte[] data = zkClient.getData("/testacl", false, null);
        System.out.println(new String(data));
    }
}
