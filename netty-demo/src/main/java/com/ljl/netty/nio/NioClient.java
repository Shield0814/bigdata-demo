package com.ljl.netty.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Date;
import java.util.Scanner;

public class NioClient {
    public static void main(String[] args) throws IOException {
        System.out.println("client");
        // 1. 获取通道
        SocketChannel sChannel = SocketChannel.open(new InetSocketAddress(9999));


        // 2.切换为非阻塞式
        sChannel.configureBlocking(false);

        // 3.分配缓冲区
        ByteBuffer buffer = ByteBuffer.allocate(1024);

        // 4.向服务端发送消息
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            buffer.clear();
            buffer.put((new Date().toString() + ":" + scanner.next()).getBytes());
            buffer.flip();
            sChannel.write(buffer);

            buffer.flip();
            int len = sChannel.read(buffer);
            System.out.println("收到服务端回应：" + new String(buffer.array(), 0, len));

        }

        System.out.println(sChannel);
        // 5.关闭通道
        sChannel.close();
    }
}
