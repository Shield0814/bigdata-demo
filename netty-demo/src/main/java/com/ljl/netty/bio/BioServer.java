package com.ljl.netty.bio;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * BIO 模式下，服务端为每个客户端创建一个线程来处理其请求
 */
public class BioServer {
    public static void main(String[] args) {

        //创建一个线程池，
        // 如果有客户端连接，就创建一个线程，与之通信
        ExecutorService pool = Executors.newCachedThreadPool();

        try {
            ServerSocket serverSocket = new ServerSocket(9999);

            System.out.println("BIO 模式服务器已经启动...");
            while (true) {

                //获得客户端连接
                Socket socket = serverSocket.accept();

                pool.execute(() -> {
                    handle(socket);
                });

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            pool.shutdown();
        }
    }


    public static void handle(Socket socket) {

        //客户端信息
        String clientInfo = socket.getInetAddress().getHostAddress() + ":" + socket.getPort();
        System.out.println(clientInfo + " 已连接,服务线程: " + Thread.currentThread().getName());
        //读数据缓冲
        byte[] buffer = new byte[1024];

        //循环获取客户端发送的数据
        try {
            while (true) {

                //1. 服务端打印客户端发送的消息
                InputStream in = socket.getInputStream();
                int len = in.read(buffer);
                if (len != -1) {
                    if (len > 1) {
                        System.out.print(clientInfo + ">" + new String(buffer, 0, len));
                        //2. 服务端回复客户端消息已经收到
                        OutputStream out = socket.getOutputStream();
                        out.write("消息已收到\n".getBytes());
                    }

                } else {
                    break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println(clientInfo + "!断开连接");
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
