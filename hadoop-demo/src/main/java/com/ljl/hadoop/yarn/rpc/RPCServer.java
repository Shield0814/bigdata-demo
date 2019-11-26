package com.ljl.hadoop.yarn.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

public class RPCServer {

    public static void main(String[] args) {
        Configuration conf = new Configuration(false);
        try {

            RPC.Server server = new RPC.Builder(conf)
                    .setProtocol(ClientProtocol.class)
                    .setInstance(new ClientProtocolImpl())
                    .setBindAddress("127.0.0.1")
                    .setPort(44444)
                    .setNumHandlers(5)
                    .build();
            server.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
