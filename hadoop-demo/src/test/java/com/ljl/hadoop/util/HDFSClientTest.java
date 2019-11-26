package com.ljl.hadoop.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClientTest {

    @Test
    public void testUpload() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        FileSystem dfs = FileSystem.get(new URI("hdfs://bigdata116:8020"), conf, "sysadm");
        Path srcPath = new Path("D:\\data\\video");
        Path dstPath = new Path("/user/hive/warehouse/yutube_video");
        dfs.copyFromLocalFile(false, srcPath, dstPath);
    }
}
