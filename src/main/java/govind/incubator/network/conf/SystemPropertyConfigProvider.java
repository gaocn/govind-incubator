package govind.incubator.network.conf;

import java.util.NoSuchElementException;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-24
 *
 * 基于{@link System#getProperty(String)}的配置提供器
 *
 */
public class SystemPropertyConfigProvider extends ConfigProvider {
	@Override
	public String get(String name) {
		String value = System.getProperty(name);
		if (value == null) {
			throw new NoSuchElementException(name);
		}
		return value;
	}
}
