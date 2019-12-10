package com.ljl.data.recursive;

public class MazeQuestion {


    public static void main(String[] args) {
        int rowSize = 8;
        int colSize = 7;
        int[][] map = new int[rowSize][colSize];

        //初始化迷宫地图
        initMap(map, rowSize, colSize);

        //显示地图
        showMap(map);
    }


    /**
     * 寻找路径
     *
     * @param map         地图
     * @param startPiontX 起点坐标 X
     * @param startPointY 起点坐标 Y
     * @param endPointX   终点坐标 X
     * @param endPointY   终点坐标 Y
     * @return
     */
    public static boolean findWay(int[][] map, int startPiontX, int startPointY, int endPointX, int endPointY) {

        return false;
    }


    /**
     * 初始化地图
     *
     * @param map
     */
    public static void initMap(int[][] map, int rowSize, int colSize) {
        for (int i = 0; i < rowSize; i++) {
            for (int j = 0; j < colSize; j++) {
                if (i == 0 || i == rowSize - 1) {
                    map[i][j] = 1;
                }
                if (j == 0 || j == colSize - 1) {
                    map[i][j] = 1;
                }
            }
        }
        map[3][1] = 1;
        map[3][2] = 1;
    }

    /**
     * 显示地图
     *
     * @param map
     */
    public static void showMap(int[][] map) {
        for (int i = 0; i < map.length; i++) {
            for (int j = 0; j < map[i].length; j++) {
                System.out.print(map[i][j] + "\t");
            }
            System.out.println();
        }
    }
}
