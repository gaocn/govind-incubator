package govind.incubator.network.conf;

import java.util.NoSuchElementException;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-24
 *
 * 配置器父类
 *
 */
public abstract class ConfigProvider {

	public abstract String get(String name);

	public String get(String name, String defaultValue) {
		try {
			return get(name);
		} catch (NoSuchElementException e) {
			return defaultValue;
		}
	}

	public boolean getBoolean(String name, boolean defaultValue) {
		return Boolean.parseBoolean(get(name, String.valueOf(defaultValue)));
	}

	public long getLong(String name, long defaultValue) {
		return Long.parseLong(get(name, String.valueOf(defaultValue)));
	}

	public int getInt(String name, int defaultValue) {
		return Integer.parseInt(get(name, String.valueOf(defaultValue)));
	}

	public double getDouble(String name, double defaultValue) {
		return Double.parseDouble(get(name, String.valueOf(defaultValue)));
	}
}
