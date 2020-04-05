package pers.qingyu.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertityUtil {

	private static Properties properties = null;

	public static Properties getProperties() throws IOException {
		InputStream is = ClassLoader.getSystemResourceAsStream("kafka.properties");

		properties = new Properties();
		properties.load(is);
		return properties;
	}
}
