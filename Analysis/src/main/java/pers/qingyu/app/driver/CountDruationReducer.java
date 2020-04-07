package pers.qingyu.app.driver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.joni.constants.Reduce;
import pers.qingyu.app.kvbean.CommonDemension;
import pers.qingyu.app.kvbean.CountDurationValue;

import java.awt.*;
import java.io.IOException;

public class CountDruationReducer extends Reducer<CommonDemension, Text, CommonDemension, CountDurationValue> {

	private int counter;
	private int durations;

	private CountDurationValue outValue = new CountDurationValue();

	@Override
	protected void reduce(CommonDemension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		super.reduce(key, values, context);
		//初始化通话时长
		counter = 0;
		durations = 0;

		//循环累加时长
		for (Text value : values) {
			counter ++;
			durations += Integer.valueOf(value.toString());
		}

		outValue.setCount(counter);
		outValue.setDuration(durations);

		context.write(key, outValue);
	}
}
