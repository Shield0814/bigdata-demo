package com.ljl.hadoop.common.value;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class CommonFriendArrayWritable extends ArrayWritable {


    public CommonFriendArrayWritable() {
        super(Text.class);
    }

    @Override
    public String toString() {
        Writable[] values = get();
        if (values.length != 0) {
            StringBuilder sb = new StringBuilder(values[0].toString());
            for (int i = 1; i < values.length; i++) {
                sb.append("," + values[i]);
            }
            return sb.toString();
        }
        return "";
    }

}
