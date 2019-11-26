package com.ljl.hive.dimension;

public class DateDimension {

    // 日期维度主键
    private int dateKey;

    private String date;

    private String dateDesc;


    //当前日期是当前月的第几天
    private String dayNumInMonth;

    //当前日期是当前年的第几天
    private String dayNumInYear;

    // 当前日期所在月的最后一天
    private String lastDayOfMonth;

    // 星期
    private String dayOfWeek;

    // 当前日期所在周是当前日期所在年的第几周
    private String weekNumInYear;

    // 当前日期所在周的开始日期，eg: 周一的日期
    private String startDateOfWeek;

    // 当前日期所在周的结束日期，eg: 周日的日期
    private String endDateOfWeek;

    // 第几季度
    private String quarter;

    // 是否是周末
    private String isWeekend;

    //是否节假日
    private String isHoliday;


}
