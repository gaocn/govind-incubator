package govind.incubator.network.sasl;

import govind.incubator.network.conf.TransportConf;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.network.server.TransportServerBootstrap;
import io.netty.channel.Channel;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-31
 *
 * A bootstrap which is executed on a TransportServer's client channel
 * once a client connects to the server. This allows customizing the
 * client channel to allow for things such as SASL authentication.
 *
 */
public class SaslServerBootstrap implements TransportServerBootstrap {

	private final TransportConf conf;
	private final SecretKeyHolder secretKeyHolder;

	public SaslServerBootstrap(TransportConf conf, SecretKeyHolder secretKeyHolder) {
		this.conf = conf;
		this.secretKeyHolder = secretKeyHolder;
	}

	/**
	 * Wrap the given application handler in a SaslRpcHandler that will
	 * handle the initial SASL negotiation.
	 *
	 * @param channel 客户端连接的且已打开的channel
	 * @param rpcHandler 服务端的RPC handler
	 * @return
	 */
	@Override
	public RpcHandler doBootstrap(Channel channel, RpcHandler rpcHandler) {
		return new SaslRpcHandler(conf, channel, rpcHandler, secretKeyHolder);
	}
}
