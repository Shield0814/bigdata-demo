package com.ljl.hadoop.util;

import org.junit.Test;

import java.util.HashSet;
import java.util.Map;

public class ParameterUtilTest {

    @Test
    public void test2() {
        HashSet<String> sets = new HashSet<>();
        sets.add("d");
    }

    @Test
    public void test1() {
        String[] splits = "4 \t13966251146\t192.168.100.1\t\t\t240\t0\t404".split("\\t");
        System.out.println(splits.length);
        int downFlow = Integer.parseInt(splits[splits.length - 2]);
        int upFlow = Integer.parseInt(splits[splits.length - 3]);
        System.out.println(downFlow);
        System.out.println(upFlow);
    }

    @Test
    public void test() {
        String[] args = {"--queue", "rwd_hive",
                "--query", "select * from emp",
                "--app", "hive-sql", "3", "--test", "smith"};
        Map<String, String> dataMap = ParameterUtil.parseArgs(args);
        String queue = ParameterUtil.getArgByKey(args, "queue");
        System.out.println(queue);
    }
}
