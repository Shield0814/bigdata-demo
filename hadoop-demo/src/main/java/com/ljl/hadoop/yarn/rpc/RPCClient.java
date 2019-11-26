package com.ljl.hadoop.yarn.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

public class RPCClient {

    public static void main(String[] args) {

        Configuration conf = new Configuration(false);

        String host = "127.0.0.1";
        int port = 44444;
        InetSocketAddress addr = new InetSocketAddress(host, port);
        ClientProtocol proxy = null;
        int count = 10;
        try {
            //通过getProxy()创建代理
            proxy = RPC.getProxy(ClientProtocol.class, ClientProtocol.versionID, addr, conf);
            //通过waitForProxy()创建代理
//            proxy = RPC.waitForProxy(ClientProtocol.class, ClientProtocol.versionID, addr, conf, 1000L);

            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNext() && count > 0) {
                scanner.next();
                String echoRes = proxy.echo("hello, this is my first rpc program");
                Integer addRes = proxy.add(10, 2);
                System.out.println("echoRes:>>" + echoRes);
                System.out.println("addRes:>>" + addRes);
                count--;
            }
            //停止RPC代理
            RPC.stopProxy(proxy);
            System.out.println("");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
