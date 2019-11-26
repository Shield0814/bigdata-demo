package com.ljl.zk.test;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.io.IOException;

public class ZKTest {

    @Test
    public void testSessionTimeout() throws IOException, KeeperException, InterruptedException {
        String connStr = "bigdata116:2181,bigdata117:2181,bigdata118:2181";
        int sessionTimeout = 10;
        ZooKeeper zkClient = new ZooKeeper(connStr, sessionTimeout, null);

        String s = zkClient.create("/test/sessionTimeoutTest", "sessionTimeoutTest".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(s);

        Thread.sleep(sessionTimeout);
        zkClient.close();

    }
}
