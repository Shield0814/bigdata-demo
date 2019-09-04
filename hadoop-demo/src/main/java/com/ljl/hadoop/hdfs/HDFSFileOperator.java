package com.ljl.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.*;

public class HDFSFileOperator implements FileOperator {


    private ExecutorService threadPool = null;
    private URI FILE_SYSTEM_URL = null;
    private String FILE_SYSTEM_USER = null;

    public HDFSFileOperator() {
        init();

    }

    /**
     * 初始化线程池，文件系统
     */
    private void init() {
        threadPool = new ThreadPoolExecutor(10, 10, 10,
                TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(100));

        Properties props = new Properties();
        InputStream in = getClass().getResourceAsStream("/sys.properties");
        try {
            props.load(in);
            FILE_SYSTEM_URL = new URI(props.getProperty("FILE_SYSTEM_URL"));
            FILE_SYSTEM_USER = props.getProperty("FILE_SYSTEM_USER");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public String uploadFile(String srcPath, String targetPath) {
//        fs.copyFromLocalFile();
        FileSystem fs = null;
        FSDataOutputStream dest = null;
        FileChannel channel = null;

        try {
            fs = FileSystem.get(FILE_SYSTEM_URL, new Configuration(), FILE_SYSTEM_USER);
            dest = fs.create(new Path(targetPath));
            channel = FileChannel.open(Paths.get(srcPath));
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            int len = channel.read(buffer);
            while (len != -1) {
                dest.write(buffer.array(), 0, len);
                buffer.flip();
                len = channel.read(buffer);
            }
            return targetPath;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (dest != null) {
                try {
                    dest.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 以块为单位对文件并行下载
     *
     * @param filePath 文件路径
     * @param dir      下载目录
     * @throws IOException 获取文件系统出错时会报 IOException
     */
    @Override
    public void downloadFile(String filePath, String dir) throws IOException {
        Path path = new Path(filePath);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(FILE_SYSTEM_URL, new Configuration(), FILE_SYSTEM_USER);
            if (fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
                CountDownLatch latch = new CountDownLatch(blocks.length);
                for (int i = 0; i < blocks.length; i++) {
                    threadPool.submit(new BlockWrapper(blocks[i], fs, path, latch, dir + "\\file"));
                }
                latch.await();
                threadPool.shutdown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                fs.close();
            }
        }

    }


    @Override
    public boolean deleteFile(String filePath, boolean recursive) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(FILE_SYSTEM_URL, new Configuration(), FILE_SYSTEM_USER);
            return fs.delete(new Path(filePath), recursive);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    @Override
    public String renameFile(String opath, String tpath) {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(FILE_SYSTEM_URL, new Configuration(), FILE_SYSTEM_USER);
            fs.rename(new Path(opath), new Path(tpath));
            return tpath;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }


}
