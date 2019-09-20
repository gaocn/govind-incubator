package govind.incubator.network.handler;

import govind.incubator.network.protocol.Message;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-20
 *
 * 消息处理器抽象类，用于处理请求或响应消息，每一个MessageHandler实
 * 例与一个Channel关联。
 *
 * PS：同一个channel中可能有多个TransportClient！
 *
 */
public abstract class MessageHandler<T extends Message> {
	/**
	 * 处理具体消息的接口
	 * @param message
	 * @throws Exception
	 */
	public abstract void handler(T message) throws Exception;

	/**
	 * 当channel上发生异常时被调用
	 * @param cause
	 * @throws Exception
	 */
	public abstract void exceptionCaught(Throwable cause) throws Exception;

	/**
	 * 当该MessageHandler实例所关联的Channel注销时被调用
	 */
	public abstract void channelUnregistered();
}
