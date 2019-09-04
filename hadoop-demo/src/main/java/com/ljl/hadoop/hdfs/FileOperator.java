package com.ljl.hadoop.hdfs;

import java.io.IOException;

public interface FileOperator {

    /**
     * 上传文件到文件服务器（文件系统）
     *
     * @param srcPath    要上传的的文件路径
     * @param targetPath 文件上传的地址（目录）
     * @return 文件上传后所在的目录
     */
    String uploadFile(String srcPath, String targetPath);

    /**
     * 下载文件到指定目录
     *
     * @param filePath 要下载文件的路径
     * @param dir      下载文件保存的目录
     */
    void downloadFile(String filePath, String dir) throws IOException;

    /**
     * 删除某个文件，
     *
     * @param filePath  要删除文件的路径，可以是目录，也可以是文件
     * @param recursive 是否递归删除文件
     * @return 是否删除成功
     */
    boolean deleteFile(String filePath, boolean recursive);

    /**
     * 重命名文件
     *
     * @param opath 文件原始名称
     * @param tpath 文件修改后的名称
     * @return 文件修改后的名称
     */
    String renameFile(String opath, String tpath);


}
