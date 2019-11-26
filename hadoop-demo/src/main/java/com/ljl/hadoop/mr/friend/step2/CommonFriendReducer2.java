package com.ljl.hadoop.mr.friend.step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CommonFriendReducer2 extends Reducer<Text, Text, Text, Text> {

    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text tmp = iter.next();
        StringBuilder sb = new StringBuilder(tmp.toString());
        while (iter.hasNext()) {
            sb.append("," + iter.next().toString());
        }
        outValue.set(sb.toString());
        context.write(key, outValue);

    }
}
