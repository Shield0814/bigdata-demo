package com.ljl.data.algorithm;

/**
 * kmp算法之匹配字符串
 */
public class KMP {

    public static void main(String[] args) {
        String srcStr = "BBC ABCDAB ABCDABCDABDE";
        String targetStr = "ABCDABD";
//        String targetStr = "BBC";

        int i = kmpMatch(srcStr, targetStr);

        System.out.println(i);
    }


    /**
     * kmp算法实现字符串匹配
     *
     * @param str1
     * @param str2
     * @return
     */
    public static int kmpMatch(String str1, String str2) {

        //1. 获得子串的部分匹配表
        int[] pmt = generatePartMatchTable(str2);

        for (int i = 0, j = 0; i < str1.length(); i++) {
            while (j > 0 && str1.charAt(i) != str2.charAt(j)) {
                j = pmt[j - 1];
            }
            if (str1.charAt(i) == str2.charAt(j)) {
                j++;
            }

            if (j == str2.length()) {
                return i - j + 1;
            }
        }

        return -1;
    }

    /**
     * 获得一个字符串的部分匹配表
     * 部分匹配表是一个长度与字符串长度相同的int数组，每个元素表示从0到当前索引的字串中前缀集合，后缀集合相同元素的长度，eg:
     * 对于 ABCDABD 字符窗来说，它的子串 ABCDA 的前缀有：[A,AB,ABC,ABCD]， 后缀有: [A,DDA,CDA,BCDA],子串结束点的索引为4
     * 前缀集合和后缀集合中的共同元素为A,A的长度为1,所以对于 ABCDABD 字符窗来说，在他的部分匹配表pmt[4]=1
     *
     * @param str2
     * @return
     */
    private static int[] generatePartMatchTable(String str2) {
        int[] pmt = new int[str2.length()];
        pmt[0] = 0;
        for (int i = 1, j = 0; i < str2.length(); i++) {
            while (j > 0 && str2.charAt(i) != str2.charAt(j)) {
                j = pmt[j - 1];
            }
            if (str2.charAt(i) == str2.charAt(j)) {
                j++;
            }
            pmt[i] = j;
        }
        return pmt;
    }


    /**
     * 字符串匹配值暴力匹配
     *
     * @param str1 源字符串
     * @param str2 要匹配的字符串
     * @return
     */
    public static int voilenceMatch(String str1, String str2) {
        char[] srcChars = str1.toCharArray();
        char[] targetChars = str2.toCharArray();

        //srcChars的下标
        int i = 0;
        //targetChars的下标
        int j = 0;
        while (i < srcChars.length && j < targetChars.length) {

            if (srcChars[i] == targetChars[j]) {
                i++;
                j++;
            } else {
                i = i - (j - 1);
                j = 0;
            }
        }
        if (j == targetChars.length) {
            return i - j;
        }
        return -1;
    }


}
