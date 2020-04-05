package pers.qingyu.coprocessor;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import pers.qingyu.constant.Constant;
import pers.qingyu.util.HBaseUtil;
import pers.qingyu.util.PropertityUtil;

import java.io.IOException;

public class MyCoprocessor extends BaseRegionObserver {

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

		//获取协处理器中的表
		String thisTable = e.getEnvironment().getRegion().getRegionInfo().getTable().getNameAsString();
		//获取当前操作的表
		String myTable = PropertityUtil.getProperties().getProperty("hbase.table.name");

		if (! thisTable.equals(myTable)){
			return;
		}

		String rowKey = Bytes.toString(put.getRow());

		String[] split = rowKey.split("_");

		if ("0".equals(split[5])){
			return;
		}

		String caller = split[1];
		String buildTime = split[2];
		String buildTS = split[3];
		String callee = split[4];
		String duration = split[6];
		int regions = Integer.valueOf(PropertityUtil.getProperties().getProperty("hbase.regions"));

		int rowHash = HBaseUtil.getRowHash(regions, callee, buildTime);

		String newRowkey = HBaseUtil.getRowkey(rowHash, callee, buildTime, buildTS, caller, duration, "0");

		Put newPut = new Put(Bytes.toBytes(newRowkey));

		newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call1"), Bytes.toBytes(callee));
		newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTime"), Bytes.toBytes(buildTime));
		newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("buildTS"), Bytes.toBytes(buildTS + ""));
		newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("call2"), Bytes.toBytes(caller));
		newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
		newPut.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("flag"), Bytes.toBytes("0"));

		Connection connection = ConnectionFactory.createConnection(Constant.CONF);
		Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(thisTable)));

		table.put(newPut);

		table.close();
		connection.close();
	}
}
