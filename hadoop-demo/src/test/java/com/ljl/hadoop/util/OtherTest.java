package com.ljl.hadoop.util;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OtherTest {

    Integer a = Integer.valueOf(127);

    Map<String, Object> map = new HashMap<>();

    @Test
    public void test() {

        Arrays.asList("").stream().map(x -> x + "");

        map.put("aaaa", "11111");
        System.out.println(map);
        map(map);
        System.out.println(map);

    }


    public void map(Map<String, Object> map) {
        map = new HashMap<>();
        map.put("aaaa", "22222");
    }

    public void a(Integer a) {

    }
}
