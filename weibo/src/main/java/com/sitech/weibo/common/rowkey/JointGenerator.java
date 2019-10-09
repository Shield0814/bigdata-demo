package com.sitech.weibo.common.rowkey;

import com.sitech.weibo.common.exception.NotSupportException;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 联合rowkey生成器，根据输入的参数以指定分隔符拼接成一个字符串
 */
public class JointGenerator implements Generator {

    private String separator = "|";

    @Override
    public byte[] getRowKey(String... params) throws NotSupportException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < params.length; i++) {
            sb.append(params[i] + separator);
        }
        if (sb.length() > KEY_MAX_LENGTH + 1) {
            throw new NotSupportException("JointGenerator rowkey最大长度不应该超过" + KEY_MAX_LENGTH);
        }
        return Bytes.toBytes(sb.substring(0, sb.lastIndexOf(separator)));
    }

    @Override
    public byte[] getRowKey() throws NotSupportException {
        throw new NotSupportException("JointGenerator 不支持通过该方法获得rowkey");
    }

    public JointGenerator(String separator) {
        this.separator = separator;
    }

    public JointGenerator() {
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }


}
