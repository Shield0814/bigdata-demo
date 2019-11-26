package com.ljl.data.algorithm;

public class DivideAndConquer {

    public static void main(String[] args) {

        hannoiTower(2, 'A', 'B', 'C');
    }


    /**
     * 分治算法思想之汉诺塔
     *
     * @param num 盘数
     * @param a   盘所在的柱子
     * @param b   辅助柱子
     * @param c   目的柱子
     */
    public static void hannoiTower(int num, char a, char b, char c) {
        if (num < 0) {
            throw new IllegalArgumentException("num 必须大于0");
        }
        if (num == 1) {
            System.out.println("第1个盘子: " + a + "->" + c);
        } else {
            //如果盘子的数量>=2, 把这些盘子看成两个，一个是最底层的1个，一个是剩下的n-1个
            //1. 先把上面的n-1个移动到辅助柱子b上
            hannoiTower(num - 1, a, c, b);

            //2. 然后把最后一个移动到目标柱子c上
            System.out.println("第" + num + "个盘子: " + a + "->" + c);

            //3. 最后把辅助柱子b上的盘子移动到目标柱子c上
            hannoiTower(num - 1, b, a, c);
        }
    }
}
