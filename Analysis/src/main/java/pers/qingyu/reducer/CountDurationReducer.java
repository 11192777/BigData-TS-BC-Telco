package pers.qingyu.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pers.qingyu.kv.key.ComDimension;
import pers.qingyu.kv.value.CountDurationValue;

import java.io.IOException;

public class CountDurationReducer<ComDimension> extends Reducer<ComDimension, Text, ComDimension, CountDurationValue>{
    CountDurationValue countDurationValue = new CountDurationValue();
    @Override
    protected void reduce(ComDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int callSum = 0;
        int callDurationSum = 0;
        for(Text t : values){
            callSum++;
            callDurationSum += Integer.valueOf(t.toString());
        }
        countDurationValue.setCallSum(String.valueOf(callSum));
        countDurationValue.setCallDurationSum(String.valueOf(callDurationSum));

        context.write(key, countDurationValue);
    }
}
