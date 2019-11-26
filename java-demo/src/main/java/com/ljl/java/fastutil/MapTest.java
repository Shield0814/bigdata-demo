package com.ljl.java.fastutil;


import it.unimi.dsi.fastutil.ints.Int2IntRBTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import org.junit.Test;

import java.util.Random;

public class MapTest {

    Random random = new Random();

    @Test
    public void testInt2IntRBTreeMap() {
        //红黑树是用非严格的平衡来换取增删节点时候旋转次数的降低开销；
        //基于红黑树的实现，更新操作快，适合更新处理和查询处理次数差不多的场景
        //基于平衡二叉树的实现，更新较慢，查询较快，适合大数据集查询处理
        long start = System.currentTimeMillis();
        Int2IntRBTreeMap int2IntRBTreeMap = new Int2IntRBTreeMap((o1, o2) -> o2.compareTo(o1));
        for (int i = 0; i < 20000000; i++) {
            int2IntRBTreeMap.put(random.nextInt(200000000), random.nextInt(10));
//            if(int2IntRBTreeMap.size() >= 10){
//                int2IntRBTreeMap.remove(int2IntRBTreeMap.lastIntKey());
//            }
        }

        System.out.println(int2IntRBTreeMap.get(10));
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        Int2ObjectAVLTreeMap<String> stringInt2ObjectAVLTreeMap = new Int2ObjectAVLTreeMap<>((o1, o2) -> o2.compareTo(o1));
        for (int i = 0; i < 20000000; i++) {
            stringInt2ObjectAVLTreeMap.put(random.nextInt(200000000), String.valueOf(random.nextInt(20)));
//            if (stringInt2ObjectAVLTreeMap.size() > 10){
//                stringInt2ObjectAVLTreeMap.remove(stringInt2ObjectAVLTreeMap.lastIntKey());
//            }
        }
        System.out.println(stringInt2ObjectAVLTreeMap.get(10));
        System.out.println(System.currentTimeMillis() - start);

    }
}
