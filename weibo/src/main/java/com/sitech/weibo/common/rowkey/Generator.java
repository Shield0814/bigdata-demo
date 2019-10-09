package com.sitech.weibo.common.rowkey;

import com.sitech.weibo.common.exception.NotSupportException;

/**
 * rowkey生成器
 */
public interface Generator {

    int KEY_MAX_LENGTH = 16 * 1024;

    byte[] getRowKey(String... params) throws NotSupportException;

    byte[] getRowKey() throws NotSupportException;
}
