package govind.incubator.network.client;

import io.netty.channel.Channel;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-26
 *
 * 在TransportClient实例返回前，调用bootstrap，可实现在每一次连接
 * 时信息的初始化交换，例如：SASL认证令牌。
 *
 * 由于TransportClient、连接会尽可能的复用，因此在初始化时执行expensive
 * bootstrapping操作是合理的，因此客户或连接通常会与JVM实例的生命周
 * 期一样长。
 *
 */
public interface TransportClientBootstrap {
	/**
	 * 在创建TransportClient实例时，调用doBootStrap对通道进行配置
	 * @param client 客户端
	 * @param channel 客户端连接的且已打开的channel
	 */
	void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;
}
