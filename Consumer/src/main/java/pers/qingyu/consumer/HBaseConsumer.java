package pers.qingyu.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pers.qingyu.util.HBaseUtil;
import pers.qingyu.util.PropertityUtil;

import javax.servlet.jsp.tagext.TryCatchFinally;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Properties;

public class HBaseConsumer {

	public static void main(String[] args) throws IOException, ParseException {

		//获取Kafka配置信息
		Properties properties = PropertityUtil.getProperties();

		//穿件Kafka消费者并订阅主题
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
		kafkaConsumer.subscribe(Collections.singletonList(properties.getProperty("kafka.topics")));


		HBaseDao dao = new HBaseDao();

		try {
			//循环拉去并打印
			while (true) {
				ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);

				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					System.out.println(consumerRecord.value());
					dao.put(consumerRecord.value());
				}
			}
		} finally {
			dao.close();
		}


	}
}
