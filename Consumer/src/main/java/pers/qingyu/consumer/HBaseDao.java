package pers.qingyu.consumer;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import pers.qingyu.constant.Constant;
import pers.qingyu.util.HBaseUtil;
import pers.qingyu.util.PropertityUtil;

import javax.xml.bind.ValidationEvent;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HBaseDao {

	private Properties properties;
	private String nameSpace;
	private String tableName;
	private int regions;
	private String cf;
	private SimpleDateFormat sdf;
	private Connection connection;
	private Table table;
	private List<Put> puts;

	private String flag;

	public HBaseDao() throws IOException {
		//初始化相应参数
		Properties properties = PropertityUtil.getProperties();
		nameSpace = properties.getProperty("hbase.namespace");
		tableName = properties.getProperty("hbase.table.name");
		regions = Integer.valueOf(properties.getProperty("hbase.regions"));
		cf = properties.getProperty("hbase.cf");
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		//初始化命名空间
		//HBaseUtil.createNamespace(nameSpace);
		//创建表
		HBaseUtil.createTable(tableName, regions, cf, "f2");

		connection = ConnectionFactory.createConnection(Constant.CONF);
		table = connection.getTable(TableName.valueOf(tableName));

		puts = new ArrayList<>();
		flag = "1";
	}

	//批量提交数据
	public void put(String value) throws ParseException, IOException {
		if (value == null) {
			return;
		}

		//17868457605,15133295266,2017-11-14 23:38:37,0411
		String[] strs = value.split(",");
		String call1 = strs[0];
		String call2 = strs[1];
		String buildTime = strs[2];
		String duration = strs[3];

		long buildTS = sdf.parse(buildTime).getTime();

		//生成分区号
		int rowHash = HBaseUtil.getRowHash(regions, call1, buildTime);
		//生成rowKey
		String rowkey = HBaseUtil.getRowkey(rowHash, call1, buildTime, buildTS + "", call2, duration, flag);

		//生成put对象
		Put put = new Put(Bytes.toBytes(rowkey));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call1"), Bytes.toBytes(call1));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS + ""));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("call2"), Bytes.toBytes(call2));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("duration"), Bytes.toBytes(duration));
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("flag"), Bytes.toBytes(flag));

		puts.add(put);

		if (puts.size() > 10){
			table.put(puts);
			puts.clear();
		}

	}

	public void close() throws IOException {
		table.put(puts);
		table.close();
		connection.close();
	}
}
