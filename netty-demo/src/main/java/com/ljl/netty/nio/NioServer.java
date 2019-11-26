package com.ljl.netty.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

public class NioServer {

    public static void main(String[] args) throws IOException {
        try {
            //1. 获取serversocketchannel
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(9999));
            //配置为非阻塞模式
            serverSocketChannel.configureBlocking(false);
            System.out.println("NIO 模式服务器已经启动...");
            //2. 获取selector
            Selector selector = Selector.open();

            ByteBuffer buffer = ByteBuffer.allocate(1024);
            //3. 把serversocketchannel注册到selector,监听 OP_ACCEPT 事件
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                if (selector.select(1000L) == 0) {
                    System.out.println("selector 1秒内没有获得连接");
                }
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    if (key.isAcceptable()) {
                        //如果key是客户端连接请求，做以下处理

                        //1. 创建一个SocketChannel
                        SocketChannel socketChn = serverSocketChannel.accept();
                        System.out.println("客户端连接:" + socketChn.socket().getInetAddress().getHostAddress() + ":"
                                + socketChn.socket().getPort());
                        socketChn.configureBlocking(false);
                        //2. 把该socketChannel注册到selector，监听 OP_READ事件,并给该SocketChannel关联一个Buffer
                        socketChn.register(selector, SelectionKey.OP_READ);
                    }
                    if (key.isReadable()) {
                        //如果key是读取事件，做以下处理
                        SocketChannel chn = (SocketChannel) key.channel();
                        buffer.clear();
                        chn.read(buffer);
                        String clientAddr = chn.socket().getInetAddress().getHostAddress();
                        int clientPort = chn.socket().getPort();
                        System.out.print(clientAddr + ":" + clientPort + ">" + new String(buffer.array()));
                        chn.register(selector, SelectionKey.OP_WRITE);
                    }
                    if (key.isWritable()) {
                        SocketChannel chn = (SocketChannel) key.channel();
                        buffer.clear();
                        buffer.put(ByteBuffer.wrap("消息已收到\n".getBytes()));
                        buffer.flip();
                        chn.write(buffer);
                    }
                    //移除SelectionKey防止重复操作

                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
