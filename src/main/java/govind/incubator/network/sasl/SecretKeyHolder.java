package govind.incubator.network.sasl;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 *
 * 获取与某个应用（appId）关联的Sasl用户和密钥
 */
public interface SecretKeyHolder {

	/**
	 * 根据{@code appId}获取对应的Sasl用户
	 * @param appId
	 * @throws IllegalArgumentException 若给定{@code appId}没有关联的Sasl用户
	 */
	String getSaslUser(String appId);

	/**
	 * 根据{@code appId}获取对应的Sasl密钥
	 * @param appId 若给定{@code appId}没有关联的Sasl密钥
	 * @throws IllegalArgumentException
	 */
	String getSecretKey(String appId);
}
