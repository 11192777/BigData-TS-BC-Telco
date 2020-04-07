package pers.qingyu.app.driver;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import pers.qingyu.app.kvbean.CommonDemension;
import pers.qingyu.app.kvbean.ContactDemension;
import pers.qingyu.app.kvbean.DateDemension;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountDurationMapper extends TableMapper<CommonDemension, Text> {

	//公共维度
	private CommonDemension commonDemension = new CommonDemension();
	//联系人维度
	private ContactDemension contactDemension = new ContactDemension();
	//日期维度
	private DateDemension dateDemension = new DateDemension();

	//值
	private Text outValue;

	private Map<String, String> contacts = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		contacts = new HashMap();
		contacts.put("15369468720", "李雁");
		contacts.put("19920860202", "卫艺");
		contacts.put("18411925860", "仰莉");
		contacts.put("14473548449", "陶欣悦");
		contacts.put("18749966182", "施梅梅");
		contacts.put("19379884788", "金虹霖");
		contacts.put("19335715448", "魏明艳");
		contacts.put("18503558939", "华贞");
		contacts.put("13407209608", "华啟倩");
		contacts.put("15596505995", "仲采绿");
		contacts.put("17519874292", "卫丹");
		contacts.put("15178485516", "戚丽红");
		contacts.put("19877232369", "何翠柔");
		contacts.put("18706287692", "钱溶艳");
		contacts.put("18944239644", "钱琳");
		contacts.put("17325302007", "缪静欣");
		contacts.put("18839074540", "焦秋菊");
		contacts.put("19879419704", "吕访琴");
		contacts.put("16480981069", "沈丹");
		contacts.put("18674257265", "褚美丽");
		contacts.put("18302820904", "孙怡");
		contacts.put("15133295266", "许婵");
		contacts.put("17868457605", "曹红恋");
		contacts.put("15490732767", "吕柔");
		contacts.put("15064972307", "冯怜云");
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		//context.write(19879419704_2017-03-27,1150);
		String rowKey = Bytes.toString(key.get());
		String[] split = rowKey.split("_");

		//针对f2列族数据不做操作
		if ("0".equals(split[5])) {
			return;
		}

		String caller = split[1];
		String buildTime = split[2];
		String callee = split[4];
		String duration = split[6];

		//2017-05-23 12:11:11
		String year = buildTime.substring(0, 4);
		String month = buildTime.substring(5, 7);
		String day = buildTime.substring(8, 10);

		//值（通话时长）
		outValue.set(duration);

		//主联系人维度
		contactDemension.setPhoneNumber(caller);
		contactDemension.setName(contacts.get(caller));
		commonDemension.setContactDemension(contactDemension);

		//年维度
		DateDemension yearDemension = new DateDemension(year, "-1", "-1");
		commonDemension.setDateDemension(yearDemension);
		context.write(commonDemension, outValue);

		//月维度
		DateDemension monthDemension = new DateDemension(year, month, "-1");
		commonDemension.setDateDemension(monthDemension);
		context.write(commonDemension, outValue);

		//日维度
		DateDemension dayDemension = new DateDemension(year, month, "-1");
		commonDemension.setDateDemension(dayDemension);
		context.write(commonDemension, outValue);


		//被叫系人维度
		contactDemension.setPhoneNumber(callee);
		contactDemension.setName(contacts.get(callee));
		commonDemension.setContactDemension(contactDemension);

		//年维度
		commonDemension.setDateDemension(yearDemension);
		context.write(commonDemension, outValue);

		//月维度
		commonDemension.setDateDemension(monthDemension);
		context.write(commonDemension, outValue);

		//日维度
		commonDemension.setDateDemension(dayDemension);
		context.write(commonDemension, outValue);

	}
}
