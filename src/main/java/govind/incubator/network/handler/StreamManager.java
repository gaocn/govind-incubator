package govind.incubator.network.handler;

import govind.incubator.network.TransportClient;
import govind.incubator.network.buffer.ManagedBuffer;
import io.netty.channel.Channel;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 */
public abstract class StreamManager {
	/**
	 * 根据stream获取数据，返回的数据会通过TCP连接发送给客户端
	 * @param streamId 通过StreamManager注册过的流
	 * @return ManagedBuffer
	 */
	public ManagedBuffer openStream(String streamId){
		throw new UnsupportedOperationException();
	}

	/**
	 * 将客户端与StreamId关联，确保该流只能被对应的客户端读取
	 * @param channel
	 * @param streamId 通过StreamManager注册过的流
	 */
	public void registerChannle(Channel channel, long streamId){}


	/**
	 * 将stream划分为若干块，分块获取该流中的某块数据
	 * @param streamId 通过StreamManager注册过的流
	 * @param chunkIdx
	 * @return
	 */
	public abstract ManagedBuffer getChunk(long streamId, int chunkIdx);

	/**
	 * 验证客户端是否可以从指定流中读取数据
	 * @param client
	 * @param streamId
	 * @throws SecurityException
	 */
	public void checkAuthorization(TransportClient client, long streamId){}

	/**
	 * 当客服端断开连接时，停止流传输并关闭，确保一个流打开一次仅对对应客户端服务
	 * @param channel
	 */
	public abstract void connectionTerminated(Channel channel);
}
