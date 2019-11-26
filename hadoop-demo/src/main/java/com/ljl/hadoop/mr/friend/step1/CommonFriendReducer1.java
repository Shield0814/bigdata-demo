package com.ljl.hadoop.mr.friend.step1;

import com.ljl.hadoop.common.value.CommonFriendArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFriendReducer1 extends Reducer<Text, Text, Text, CommonFriendArrayWritable> {

    private CommonFriendArrayWritable outValue = new CommonFriendArrayWritable();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> tmp = new ArrayList<>();
        for (Text value : values) {
            tmp.add(new Text(value));
        }
        Text[] result = tmp.toArray(new Text[0]);
        outValue.set(result);
        context.write(key, outValue);
    }
}
