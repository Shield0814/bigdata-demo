package com.ljl.netty.nio.groupchat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

public class GroupChatServer {

    private Selector selector;

    private ServerSocketChannel ssc;

    private static final int PORT = 6667;

    private ByteBuffer readBuffer;


    public GroupChatServer() {
        initServer();
    }

    //初始化服务器
    void initServer() {
        try {
            //初始化缓存
            readBuffer = ByteBuffer.allocate(1024);


            //初始化selector
            selector = Selector.open();
            //初始化serversocketchannel
            ssc = ServerSocketChannel.open();
            ssc.configureBlocking(false);
            ssc.bind(new InetSocketAddress(PORT));

            //注册ssc到selector，监听 OP_ACCEPT
            ssc.register(selector, SelectionKey.OP_ACCEPT);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void listen() {

        while (true) {
            try {
                int count = selector.select(2000L);
                if (count > 0) {

                    Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                    while (iter.hasNext()) {
                        SelectionKey key = iter.next();
                        doAccept(key);
                        doRead(key);
                        iter.remove();
                    }

                }
            } catch (IOException e) {
                System.out.println("客户端下线了");
            }
        }

    }

    /**
     * 处理客户端连接事件
     *
     * @param key
     */
    public void doAccept(SelectionKey key) {
        try {
            if (key.isAcceptable()) {
                SocketChannel sc = ssc.accept();
                sc.configureBlocking(false);
                sc.register(selector, SelectionKey.OP_READ);

                System.out.println(sc.getRemoteAddress().toString().substring(1) + " 上线");
            }
        } catch (IOException e) {
            System.out.println("doAccept IOException");
        }

    }

    /**
     * 处理读取客户端发送消息事件
     *
     * @param key
     */
    public void doRead(SelectionKey key) {
        SocketChannel sc = null;


        try {
            if (key.isReadable()) {
                sc = (SocketChannel) key.channel();
                readBuffer.clear();
                int len = sc.read(readBuffer);
                if (len > 0) {
                    readBuffer.flip();
                    String msg = new String(readBuffer.array());
                    System.out.println("from 客户端" + sc.getRemoteAddress() + ":" + msg);

                    //向其他客户端转发消息
                    sendMsg2OtherClients(msg, sc);

                }
            }
        } catch (IOException e) {
            System.out.println("doRead IOException");
            key.cancel();
            try {
                sc.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

    }


    /**
     * 发送消息给其他客户端
     *
     * @param msg  消息内容
     * @param self 消息来源客户端
     */
    public void sendMsg2OtherClients(String msg, SocketChannel self) {
        for (SelectionKey key : selector.keys()) {
            SelectableChannel tgtChannel = key.channel();

            try {
                if (tgtChannel instanceof SocketChannel && tgtChannel != self) {
                    SocketChannel destSc = (SocketChannel) tgtChannel;
                    String sender = self.getRemoteAddress().toString().substring(1);
                    destSc.write(ByteBuffer.wrap((sender + "说:" + msg).getBytes()));
                }
            } catch (IOException e) {
                try {
                    System.out.println(((SocketChannel) tgtChannel).getRemoteAddress() + "下线了");
                    key.cancel();
                    tgtChannel.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        GroupChatServer server = new GroupChatServer();
        server.listen();
    }

}
