package govind.incubator.shuffle.sasl;

import govind.incubator.network.sasl.SecretKeyHolder;
import govind.incubator.network.util.NettyUtil;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 用于管理shuffle服务通信时所使用到密钥
 *
 */
@Slf4j
public class ShuffleSecretManager implements SecretKeyHolder {
	/**使用shuffle服务进行安全通信时默认的SASL用户名*/
	private static final String DEFAULT_SHUFFLE_SASL_USER = "defaultShuffleSaslUser";
	private final ConcurrentHashMap<String, String> shuffleSecretMap;

	public ShuffleSecretManager() {
		this.shuffleSecretMap = new ConcurrentHashMap<>();
	}

	/**
	 * 返回默认用户用于SASL连接认证
	 */
	@Override
	public String getSaslUser(String appId) {
		return DEFAULT_SHUFFLE_SASL_USER;
	}

	/**
	 * 返回已注册应用app的密密钥。
	 *
	 * 例如：在Spark中该密钥用于认证Executor，在Executors尝试获取shuffle文件之前，
	 * 需要验证该Executor尝试获取的文件是该应用app生成的文件，若应用没有注册，则返回空。
	 *
	 */
	@Override
	public String getSecretKey(String appId) {
		return shuffleSecretMap.get(appId);
	}


	/**
	 * 注册应用并指定密钥，当某个Executor尝试获取同一个应用的其他Executors生成的文件时，
	 * 需要采用相同的密钥与Shuffle服务通信并尝试获取数据。
	 *
	 * @param appId 注册应用
	 * @param shuffleSecret 通信密钥
	 */
	public void registerApp(String appId, String shuffleSecret) {
		if (!shuffleSecretMap.contains(appId)) {
			shuffleSecretMap.put(appId, shuffleSecret);
			log.info("为应用{}注册shuffle密钥", appId);
		} else  {
			log.info("应用{}shuffle密钥已存在", appId);
		}
	}

	public void registerApp(String appId, ByteBuffer shuffleSecret) {
		registerApp(appId, NettyUtil.bytesToString(shuffleSecret));
	}

	/**
	 * 注销应用的shuffle密钥
	 */
	public void unregisterApp(String appId) {
		if (shuffleSecretMap.contains(appId)) {
			shuffleSecretMap.remove(appId);
			log.info("为应用{}注册移除shuffle密钥", appId);
		} else {
			log.warn("尝试移除未注册应用{}的shuffle密钥", appId);
		}
	}
}
