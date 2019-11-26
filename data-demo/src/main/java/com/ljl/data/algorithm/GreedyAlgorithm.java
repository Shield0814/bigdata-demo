package com.ljl.data.algorithm;

import java.util.*;

/**
 * 贪心算法解决集合覆盖问题
 * 假设存在如下表的需要付费的广播台，以及广播台信号可以覆盖的地区。
 * 如何选择最少的广播台，让所有的地区都可以接收到信号
 */
public class GreedyAlgorithm {

    public static void main(String[] args) {
        HashMap<String, List<String>> areaByBroadcast = new HashMap<>();
        areaByBroadcast.put("k1", Arrays.asList("北京", "上海", "天津"));
        areaByBroadcast.put("k2", Arrays.asList("北京", "广州", "深圳"));
        areaByBroadcast.put("k3", Arrays.asList("成都", "上海", "杭州"));
        areaByBroadcast.put("k4", Arrays.asList("上海", "天津"));
        areaByBroadcast.put("k5", Arrays.asList("杭州", "大连"));

        //要覆盖的所有区域
        HashSet<String> allArea = new HashSet<>();
        areaByBroadcast.values().forEach(allArea::addAll);
        System.out.println(allArea);

        //已选择的电台
        ArrayList<String> selectedBroadcast = new ArrayList<>(5);

        //如果allArea不为空表示已选择的电台还未覆盖所有区域
        while (!allArea.isEmpty()) {
            String maxKey = "k1";

            for (Map.Entry<String, List<String>> entry : areaByBroadcast.entrySet()) {
                long maxKeyCount = areaByBroadcast.get(maxKey).stream().filter(area -> allArea.contains(area)).count();
                //该电台覆盖的区域在未覆盖区域中的数量
                long areaCount = entry.getValue().stream().filter(area -> allArea.contains(area)).count();
                if (maxKeyCount < areaCount) {
                    maxKey = entry.getKey();
                }
            }
            //把maxkey对应的电台加入已选择电台列表,并从allArea中移除maxkey对应的区域
            selectedBroadcast.add(maxKey);
            areaByBroadcast.get(maxKey).forEach(allArea::remove);
        }

        System.out.println(selectedBroadcast);
    }
}
