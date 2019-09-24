package govind.incubator.network.conf;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-24
 *
 * 基于HashMap的配置器
 *
 */
public class MapConfigProvider extends ConfigProvider{

	private Map<String, String> config;

	public MapConfigProvider(Map<String, String> config) {
		this.config = Maps.newHashMap(config);
	}

	@Override
	public String get(String name) {
		String value = config.get(name);

		if (value == null) {
			throw new NoSuchElementException(name);
		}
		return value;
	}
}
