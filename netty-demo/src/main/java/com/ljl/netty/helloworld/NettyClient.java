package com.ljl.netty.helloworld;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public class NettyClient {
    private static final int PORT = 6668;
    private static final String HOST = "127.0.0.1";

    public static void main(String[] args) {


        //创建客户端事件循环组
        NioEventLoopGroup eventExecutors = new NioEventLoopGroup();

        try {
            //创建并配置客户端启动对象： Bootstrap  注意不是ServerBootstrap
            Bootstrap bootstrap = new Bootstrap();

            bootstrap.group(eventExecutors)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel schn) throws Exception {
                            schn.pipeline().addLast(new NettyClientHandler());
                        }
                    });

            System.out.println("客户端ok~");
            ChannelFuture cf = bootstrap.connect(new InetSocketAddress(HOST, PORT)).sync();
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            eventExecutors.shutdownGracefully();
        }

    }
}
