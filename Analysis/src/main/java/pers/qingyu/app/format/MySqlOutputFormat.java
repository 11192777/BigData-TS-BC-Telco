package pers.qingyu.app.format;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.util.Progressable;
import pers.qingyu.app.convertor.DinmesionConvertor;
import pers.qingyu.app.kvbean.CommonDemension;
import pers.qingyu.app.kvbean.CountDurationValue;

import java.io.IOException;
import java.security.Key;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class MySqlOutputFormat extends OutputFormat<CommonDemension, CountDurationValue> {

	private FileOutputCommitter committer = null;

	@Override
	public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new MysqlRecordReader();
	}

	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		if (this.committer == null) {
			Path output = getOutputPath(taskAttemptContext);
			this.committer = new FileOutputCommitter(output, taskAttemptContext);
		}

		return this.committer;
	}

	public static Path getOutputPath(JobContext job) {
		String name = job.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir");
		return name == null ? null : new Path(name);
	}

	private static class MysqlRecordReader extends RecordWriter<CommonDemension, CountDurationValue>{

		private PreparedStatement preparedStatement = null;

		//向sql注入数据的核心方法
		@Override
			public void write(CommonDemension commonDemension, CountDurationValue countDurationValue) throws IOException, InterruptedException {

			DinmesionConvertor convertor = new DinmesionConvertor();

			//1.查询维度维度ID
			int contactId = convertor.getDimensionId(commonDemension.getContactDemension());
			int dataId = convertor.getDimensionId(commonDemension.getDateDemension());
			//2.获取通过次数&通话时长

			//3.生成sql

			//4.预编译sql语句

			//5.执行（批量操作）
		}

		@Override
		public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

		}
	}
}
