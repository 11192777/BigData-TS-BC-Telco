package pers.qingyu.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ScanUtil {


	public static List<String[]> getStartStopRowKey(String phoneNum, String start, String stop) throws IOException, ParseException {

		List<String[]> list = new ArrayList<String[]>();

		//06_16480981069_2017-12-31_1514667562000_1
		//2017-12
		//2018-3

		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM");
		Date startDate = sf.parse(start);
		Date stopDate = sf.parse(stop);

		Calendar startPoint = Calendar.getInstance();
		startPoint.setTime(startDate);

		Calendar stopPoint = Calendar.getInstance();
		stopPoint.setTime(stopDate);
		stopPoint.add(Calendar.MONTH, 1);

		while (startPoint.getTimeInMillis() < stopPoint.getTimeInMillis()){

			String[] str = new String[2];

			int startRowHash = HBaseUtil.getRowHash(Integer.valueOf(PropertityUtil.getProperties().getProperty("hbase.regions")), phoneNum, sf.format(startPoint.getTime()));
			str[0] = String.format("%02d_%s_%s", startRowHash, phoneNum, sf.format(startPoint.getTime()));
			startPoint.add(Calendar.MONTH, 1);
			int stopRowHash = HBaseUtil.getRowHash(Integer.valueOf(PropertityUtil.getProperties().getProperty("hbase.regions")), phoneNum, sf.format(startPoint.getTime()));
			str[1] = String.format("%02d_%s_%s", stopRowHash, phoneNum, sf.format(startPoint.getTime()));

			list.add(str);
		}

		return list;
	}

	public static void main(String[] args) throws IOException, ParseException {
		List<String[]> rows = getStartStopRowKey("16480981069", "2017-12", "2020-6");

		for (String[] row : rows) {
			System.out.println(row[0] + "-------" + row[1]);

		}

	}
}