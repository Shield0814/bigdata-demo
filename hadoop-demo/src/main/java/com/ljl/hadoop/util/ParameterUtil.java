package com.ljl.hadoop.util;

import java.util.HashMap;
import java.util.Map;

public class ParameterUtil {

    //缓存解析出的键值对
    private volatile static Map<String, String> cacheMapData = new HashMap<>();

    /**
     * 根据输入的参数数组把参数解析成一个map,k-v对按--分割，k，v按空格分割，eg:
     * --queue rwd_hive --num_reducer 5 解析成 Map[queue->rwd_hive,num_reducer->5]
     *
     * @param args 输入的参数列表
     * @return 从输入参数中提取的键值对
     */
    public static Map<String, String> parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--") && !args[i + 1].startsWith("--")) {
                cacheMapData.put(args[i].substring(2), args[i + 1]);
                i++;
            }
        }
        return cacheMapData;
    }

    /**
     * 根据索引获取相应参数
     *
     * @param args 输入的参数列表
     * @param idx  需要获取参数的索引
     * @return 获取到的参数，如果不存在返回空
     */
    public static String getArgByIndex(String[] args, int idx) {
        if (idx >= args.length) {
            return null;
        }
        return args[idx];
    }

    /**
     * 通过key来获取相应参数，如果缓存中存在则直接返回，否则解析并返回，如果不存在返回null
     *
     * @param args 输入的参数列表
     * @param key  需要获取参数的key
     * @return 获取到的参数，如果不存在返回空
     */
    public static String getArgByKey(String[] args, String key) {
        if (cacheMapData.containsKey(key)) {
            return cacheMapData.get(key);
        } else {
            parseArgs(args);
            return cacheMapData.containsKey(key) ? cacheMapData.get(key) : null;
        }
    }


}
