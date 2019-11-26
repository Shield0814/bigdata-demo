package com.ljl.netty.nio;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class FileChannelTest {

    public static void main(String[] args) throws IOException {

        FileChannel chn = FileChannel.open(Paths.get(""), StandardOpenOption.APPEND);
    }
}
