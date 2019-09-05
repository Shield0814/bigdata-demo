package com.ljl.hadoop.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

public class RandomTest {

    @Test
    public void test() {
        Random random = new Random();
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < 1000000; i++) {
            int tmp = random.nextInt(5);
            if (map.containsKey(tmp)) {
                map.put(tmp, map.get(tmp) + 1);
            } else {
                map.put(tmp, 1);
            }
        }
        int i = 0;
    }
}
