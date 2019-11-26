package com.ljl.hadoop.util;

import org.junit.Test;

import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

public class RandomTest {

    @Test
    public void test() {
        // date为2013-09-19 14:22:30
        Date date = new Date();

        // 创建“简体中文”的Locale
        Locale localeCN = Locale.SIMPLIFIED_CHINESE;
        // 创建“英文/美国”的Locale
        Locale localeUS = new Locale("en", "US");

        // 获取“简体中文”对应的date字符串
        String cn = DateFormat.getDateInstance(DateFormat.FULL, localeCN).format(date);
        // 获取“英文/美国”对应的date字符串
        String us = DateFormat.getDateInstance(DateFormat.FULL, localeUS).format(date);

        System.out.printf("cn=%s\nus=%s\n", cn, us);

    }
}
