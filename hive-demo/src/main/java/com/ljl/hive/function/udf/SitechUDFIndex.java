package com.ljl.hive.function.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "sitech_index", value = "_FUNC_(arr,idx) - " +
        "根据传入的索引，从数组中获取对应位置的值",
        extended = "select sitech_index(arr,1)")
public class SitechUDFIndex extends UDF {

    public Object evaluate(Object[] arr, Integer idx) {
        if (idx >= arr.length || idx < 0) {
            return null;
        }
        return arr[idx];
    }
}
