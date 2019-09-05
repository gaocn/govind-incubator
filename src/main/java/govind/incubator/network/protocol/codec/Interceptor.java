package govind.incubator.network.protocol.codec;

import io.netty.buffer.ByteBuf;

/**
 *
 */
public interface Interceptor {
	/**
	 * 处理接收到的数据
	 * @param buf 包含数据的缓冲区
	 * @return 若拦截器需要继续消费数据则true，返回false则表示要卸
	 * 载handler上的拦截器
	 * @throws Exception
	 */
	boolean handle(ByteBuf buf);

	/**
	 * 当channel关闭但拦截器仍然没有卸载时调用
	 * @throws Exception
	 */
	void channelInactive();

	/**
	 * 当channel pipeline中发生异常时调用
	 * @param cause
	 * @throws Exception
	 */
	void exceptionCaught(Throwable cause);
}
