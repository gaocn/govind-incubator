package govind.incubator.network.handler;

import govind.incubator.network.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 *
 * 仅仅用于客户端Rpc处理， 不能接收Rpc消息
 *
 */
public class NoOpRpcHandler extends RpcHandler {
	private final StreamManager streamManager;

	public NoOpRpcHandler() {
		this.streamManager = new OneForOneStreamManager();
	}

	@Override
	public StreamManager getStreamManager() {
		return streamManager;
	}

	@Override
	public void receive(TransportClient client, ByteBuffer msg, RpcCallback callback) {
		throw new UnsupportedOperationException("无法处理消息");
	}
}
