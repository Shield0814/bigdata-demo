package com.sitech.gmall;

import org.junit.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class CommonTests {

    @Test
    public void test() {
        Instant date = Instant.now();//代替date
        System.out.println("instant:" + date);
        LocalDate date2 = LocalDate.now();
        System.out.println("LocalDate:" + date2);
        LocalDateTime date3 = LocalDateTime.now();//代替calendar
        System.out.println("LocalDateTime:" + date3);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");//代替simpleDateFormat
        System.out.println("DateTimeFormatter:" + dtf.format(date3));


    }
}
