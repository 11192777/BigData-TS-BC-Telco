import javafx.scene.control.Tab;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pers.qingyu.constant.Constant;
import pers.qingyu.util.PropertityUtil;
import pers.qingyu.util.ScanUtil;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class ScanTest2 {


	public static void main(String[] args) throws IOException, ParseException {

		List<String[]> rowKeys = ScanUtil.getStartStopRowKey("15369468720", "2017-04", "2017-06");

		Connection connection = ConnectionFactory.createConnection(Constant.CONF);
		Table table = connection.getTable(TableName.valueOf(PropertityUtil.getProperties().getProperty("hbase.table.name")));


		for (String[] rowKey : rowKeys) {
			Scan scan = new Scan(Bytes.toBytes(rowKey[0]), Bytes.toBytes(rowKey[1]));

			System.out.println(rowKey[0] + "-------------------" + rowKey[1]);

			ResultScanner resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				System.out.println(Bytes.toString(result.getRow()));
			}

		}

	}
}
