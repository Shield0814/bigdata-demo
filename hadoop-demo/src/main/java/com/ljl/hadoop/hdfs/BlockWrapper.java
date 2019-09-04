package com.ljl.hadoop.hdfs;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;

public class BlockWrapper implements Runnable {

    private BlockLocation block;
    private FileSystem fs;
    private Path path;
    private CountDownLatch latch;
    private String prefix;

    public BlockWrapper(BlockLocation block, FileSystem fs, Path path, CountDownLatch latch, String prefix) {
        this.block = block;
        this.fs = fs;
        this.path = path;
        this.latch = latch;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        FSDataInputStream in = null;
        FileChannel channel = null;
        try {
            long offset = block.getOffset();
            long length = block.getLength();
            in = fs.open(path);

            channel = new FileOutputStream(prefix + "_part_" + offset).getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            in.seek(offset);
            long haveRead = 0;
            int len = in.read(buffer);
            buffer.flip();
            haveRead += len;
            while (len != -1 && haveRead <= length) {
                channel.write(buffer);
                buffer.clear();
                len = in.read(buffer);
                haveRead += len;
                buffer.flip();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                channel.close();
                latch.countDown();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NullPointerException e) {
                e.printStackTrace();
            }

        }


    }
}
