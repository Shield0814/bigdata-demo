package com.ljl.netty.nio.groupchat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Scanner;

public class GroupChatClient {

    private final int PORT = 6667;
    private final String IP = "127.0.0.1";

    private Selector selector;

    private SocketChannel sc;

    private String userName;
    ByteBuffer buffer = ByteBuffer.allocate(1024);

    public GroupChatClient() {
        try {
            selector = Selector.open();
            sc = SocketChannel.open(new InetSocketAddress(IP, PORT));
            sc.configureBlocking(false);

            sc.register(selector, SelectionKey.OP_READ);
            userName = sc.getLocalAddress().toString().substring(1);
            System.out.println(userName + " is ok ...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendMsgToServer(String msg) {
        try {
            sc.write(ByteBuffer.wrap(msg.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readInfo() {
        try {
            int count = selector.select(1000);
            if (count > 0) {
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    if (key.isReadable()) {
                        SocketChannel sc = (SocketChannel) key.channel();
                        buffer.clear();
                        sc.read(buffer);
                        String msg = new String(buffer.array());
                        System.out.println(msg);
                    }
                }
            } else {
//                System.out.println("群聊无消息...");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        GroupChatClient client = new GroupChatClient();

        new Thread(() -> {
            while (true) {
                client.readInfo();
                try {
                    Thread.currentThread().sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }).start();

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String msg = scanner.next();
            client.sendMsgToServer(msg);
        }
    }
}
