package pers.qingyu.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import javax.sound.midi.Soundbank;
import java.io.IOException;
import java.text.DecimalFormat;

public class HBaseUtil {

	private static Configuration conf;

	static {
		conf = HBaseConfiguration.create();
	}

	//创建命名空间
	public static void createNamespace(String nameSpace) throws IOException {

		//获取连接对象
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		//获取命名空间描述器
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
		admin.createNamespace(namespaceDescriptor);

		//释放资源
		admin.close();
		connection.close();
	}

	//判断表是否存在
	public static boolean existTable(String tableName) throws IOException {

		//获取连接对象
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		//判断表是否存在
		boolean tableExists = admin.tableExists(TableName.valueOf(tableName));

		admin.close();
		connection.close();

		return tableExists;
	}

	//创建表
	public static void createTable(String tableName, int regions, String... cfs) throws IOException {

		//获取连接对象
		Connection connection = ConnectionFactory.createConnection(conf);
		Admin admin = connection.getAdmin();

		//判断表是否存在
		if (existTable(tableName)) {
			System.out.println("表" + tableName + "已经存在！");
			admin.close();
			connection.close();
			return;
		}

		//创建表描述其
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		//循环添加列族
		for (String cf : cfs) {
			//创建列族描述器
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}

		//创建表
		hTableDescriptor.addCoprocessor("pers.qingyu.coprocessor.MyCoprocessor");
		admin.createTable(hTableDescriptor, getSplitKeys(regions));

		admin.close();
		connection.close();
	}


	//预分区键的生成
	public static byte[][] getSplitKeys(int regions) {

		//创建分区间二维数组
		byte[][] splitKeys = new byte[regions][];
		DecimalFormat df = new DecimalFormat("00");

		//循环添加分区键
		for (int i = 0; i < regions; i++) {
			splitKeys[i] = Bytes.toBytes(df.format(i) + "|");
		}

		return splitKeys;
	}


	//生成rowKey
	//0x_13796714022_2017-05-20 12:22:23_时间戳_110200200020_duration
	public static String getRowkey(int rowHash, String caller, String buildTime, String buildTS, String callee, String duration, String flag) {

		return String.format("%02d_%s_%s_%s_%s_%s_%s", rowHash, caller,buildTime.substring(0,10),buildTS, callee, flag, duration );
	}

	//生成分区号
	public static int getRowHash(int regions, String caller, String buildTime) {

		int hashCode = new StringBuilder(caller.substring(3, 9) + buildTime.substring(0, 6)).toString().hashCode();
		return Math.abs(hashCode) % (regions + 1);
	}




}
