package com.ljl.hadoop.mr.friend.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mr练习之公共好友，job串联实现:
 * 1. job1 找出某个人是哪些人的共同好友
 * 2. job2 读取 job1 的输出，并和原始数据关联找出两两之间是好友的记录
 */
public class CommonFriendMapper1 extends Mapper<Text, Text, Text, Text> {

    private Text outKey = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        //某个人的好友
        String[] friends = value.toString().split(",");

        //逆向思维：如果某个人的好友有n个，那么这个人是他的n个好友的好友
        for (int i = 0; i < friends.length; i++) {
            outKey.set(friends[i]);
            context.write(outKey, key);
        }
    }
}
