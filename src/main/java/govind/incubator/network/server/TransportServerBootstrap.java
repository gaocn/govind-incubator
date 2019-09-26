package govind.incubator.network.server;

import govind.incubator.network.handler.RpcHandler;
import io.netty.channel.Channel;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-25
 *
 * 当客户端连接到服务端时，服务端会在{@link TransportServer}的client
 * channel上执行该bootstrap操作，以便能够自定义client channel，可以
 * 方便添加类似SASL认证功能
 *
 */
public interface TransportServerBootstrap {
	/**
	 * 自定义channel以便添加新特性
	 * @param channel 客户端连接的且已打开的channel
	 * @param rpcHandler 服务端的RPC handler
	 * @return channel使用的RPC handler
	 */
	RpcHandler doBootstrap(Channel channel, RpcHandler  rpcHandler);
}
