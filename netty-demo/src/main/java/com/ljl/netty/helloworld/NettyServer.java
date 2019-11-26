package com.ljl.netty.helloworld;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyServer {

    private static final int PORT = 6668;

    public static void main(String[] args) {

        //1. 创建bossgroup和workergroup
        //bossGroup 只负责处理处理客户端连接请求，
        //workerGroup 真正负责客户端业务处理
        //bossGroup 和 workerGroup都是无限循环的
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup(24);
        try {

            //2. 创建并配置服务端启动对象：ServerBootstrap
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class) //使用NioServerSocketChannel作为服务端的通道实现
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel schn) throws Exception {
                            schn.pipeline().addLast(new NettyServerHandler());
                        }
                    });

            System.out.println("服务端ok~");
            //3. 绑定端口并启动服务器
            ChannelFuture cf = bootstrap.bind(PORT).sync();
            cf.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            bossGroup.shutdownGracefully();
        }


    }
}
