package govind.incubator.network.handler;

import com.google.common.base.Preconditions;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.buffer.ManagedBuffer;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-19
 */
@Slf4j
public class OneForOneStreamManager extends StreamManager{
	private static class StreamState {
		final String appId;
		final Iterator<ManagedBuffer> buffers;

		/**
		 * 关联的通道
		 */
		Channel associatedChannel = null;

		/**
		 * 记录当前块标识，必须顺序读取块数据
		 */
		int curChunkIdx = 0;

		public StreamState(String appId, Iterator<ManagedBuffer> buffers) {
			this.appId = appId;
			this.buffers = buffers;
		}
	}

	/**
	 * 生成streamId，每次注册流时会产生一个Id
	 */
	final AtomicLong nextStreamId;

	/**
	 * 保存streamId与StreamState的对应关系
	 */
	final ConcurrentHashMap<Long, StreamState> streams;

	/******************************************************/

	public OneForOneStreamManager() {
		nextStreamId = new AtomicLong(
				new Random().nextInt(Integer.MAX_VALUE)*1000
		);
		streams = new ConcurrentHashMap<>();
	}

	/**
	 * 注册流，当指定appId时只有该客户端允许消费该流中的数据
	 * @param appId 客户端标识
	 * @param buffers
	 * @return streamId
	 */
	public long registerStream(String appId, Iterator<ManagedBuffer> buffers) {
		long streamId = nextStreamId.getAndIncrement();
		streams.put(streamId, new StreamState(appId, buffers));
		return streamId;
	}

	@Override
	public void registerChannle(Channel channel, long streamId) {
		if (streams.containsKey(streamId)) {
			streams.get(streamId).associatedChannel = channel;
		}
	}

	@Override
	public ManagedBuffer getChunk(long streamId, int chunkIdx) {
		StreamState streamState = streams.get(streamId);
		Preconditions.checkNotNull(streamState, String.format("无法获取streamId=%s对应的流", streamId));

		if (streamState.curChunkIdx != chunkIdx) {
			throw new IllegalArgumentException(String.format("接收到无序请求chunk索引(%s)，期待接收chunk索引(%s)", chunkIdx, streamState.curChunkIdx));
		} else if (!streamState.buffers.hasNext()) {
			throw new IllegalArgumentException(String.format("接收到的chunk索引(%s)越界", chunkIdx));
		}

		streamState.curChunkIdx += 1;
		ManagedBuffer buffer = streamState.buffers.next();
		if (!streamState.buffers.hasNext()) {
			log.info("删除streamid为{}的流，已被消费完毕", streamId);
			streams.remove(streamId);
		}
		return buffer;
	}

	/**
	 * 释放与该通道关联的流
	 * @param channel
	 */
	@Override
	public void connectionTerminated(Channel channel) {
		streams.entrySet().forEach(entry -> {
			if (entry.getValue().associatedChannel == channel) {
				streams.remove(entry.getKey());
				entry.getValue().buffers.forEachRemaining(buf->buf.release());
			}
		});
		log.info("连接{}关闭，释放该连接关联的流", channel.remoteAddress());
	}

	@Override
	public void checkAuthorization(TransportClient client, long streamId) {
		String appId = client.getClientId();
		if (StringUtils.isNoneEmpty(appId)) {
			StreamState streamState = streams.get(streamId);
			Preconditions.checkArgument(streamState != null, "访问不存在的Stream，streamId=" + streamId);
			if (!appId.equals(streamState.appId)) {
				throw new SecurityException(String.format("不允许客户端app=%s访问streamId=%s得流(只允许客户端appId=%s访问)！", appId, streamId, streamState.appId));
			}
		}
	}
}
