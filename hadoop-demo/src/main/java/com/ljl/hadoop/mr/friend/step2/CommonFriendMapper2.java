package com.ljl.hadoop.mr.friend.step2;

import com.ljl.hadoop.util.SortUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommonFriendMapper2 extends Mapper<Text, Text, Text, Text> {

    private Text outKey = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");
        SortUtil.bubbleSort(splits, (o1, o2) -> o1.compareTo(o2), true);
        for (int i = 0; i < splits.length; i++) {
            for (int j = i + 1; j < splits.length; j++) {
                outKey.set(splits[i] + "&" + splits[j]);
                context.write(outKey, key);
            }
        }
    }
}
