package com.sitech.gmall;

import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class JedisTests {

    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    Jedis jedis = new Jedis("bigdata116", 6379);

    @Test
    public void test() throws ParseException {

        String tomorrow = df.format(new Date(System.currentTimeMillis() + 24 * 3600000L));
        long tomorrowStart = df.parse(tomorrow).getTime() / 1000;
        System.out.println(tomorrowStart);

        System.out.println(jedis.ping());

        jedis.expireAt("test_ttl", tomorrowStart);

    }

    @Test
    public void test2() {
        jedis.sadd("test_ttl", "smith");
    }

}
