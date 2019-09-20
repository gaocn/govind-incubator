package govind.incubator.network.handler;

import govind.incubator.network.TransportClient;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 *
 * 服务端用于处理TransportClient.sendRpc()发送的消息的处理器
 *
 */
@Slf4j
public abstract  class RpcHandler {

	/**
	 * 针对不关心响应的请求，直接打印日志，不做任何处理
	 */
	private final static RpcCallback ONE_WAY_RPCCALLBACK = new OneWayRpcCallback();

	/**
	 * 返回当前client正在获取的流所在的StreamManager
	 * @return
	 */
	public abstract StreamManager getStreamManager();

	/**
	 * 接收一条RPC消息，该方法中的任何异常将导致RPC调用失败并将异常信
	 * 息以字符串形式返回给客户端。
	 *
	 * 对于任意一个{@link TransportClient}来说，该方法不会被并行调用
	 *
	 * @param client channel中的client端，用于在RpcHandler中与发送方进行通信
	 * @param msg RPC消息序列化后的结果
	 * @param callback 当RPC调用成功或失败时会被调用且只被调用一次
	 */
	public abstract void receive(TransportClient client, ByteBuffer msg, RpcCallback callback);

	/**
	 * 接收RPC消息并且不期待回复。
	 * 当任何回调函数被调用时，仅仅记录一条日志而不进行回复。
	 *
	 * @param client channel中的client端，用于在RpcHandler中与发送方进行通信
	 * @param msg RPC消息序列化后的结果
	 */
	public void receive(TransportClient client, ByteBuffer msg) {
		receive(client, msg, ONE_WAY_RPCCALLBACK);
	}

	/**
	 * 客户端断开连接
	 * @param client
	 */
	public void connectionTerminated(TransportClient client){}

	/**
	 * 服务端处理接收消息时发生错误
	 * @param cause
	 * @param client
	 */
	public void exceptionCaught(Throwable cause, TransportClient client){}

	/**
	 * 当接收到OneWayMessage时，仅仅打印信息，不做任何处理，用于处理只发送不关心响应的请求。
	 */
	private static class OneWayRpcCallback implements RpcCallback {
		@Override
		public void onSuccess(ByteBuffer response) {
			log.warn("Response Message provided for one-way RPC.");
		}

		@Override
		public void onFailure(Throwable cause) {
			log.warn("Response Message provided for one-way RPC.");
		}
	}
}
