package govind.incubator.shuffle;

import com.google.common.base.Preconditions;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.shuffle.protocol.BlockTransferMessage;
import govind.incubator.shuffle.protocol.OpenBlock;
import govind.incubator.shuffle.protocol.StreamHandle;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * 封装client请求block，用于将每一个block解析为一个chunk，并在解析成功或失败
 * 时调用{@link BlockFetchingListener}中的方法通知用户。
 *
 */
@Slf4j
public class OneForOneBlockFetcher {
	private final TransportClient client;
	private final OpenBlock openMessage;
	private final String[] blockIds;
	private final BlockFetchingListener listener;
	private final ChunkReceivedCallback chunkCallback;

	private StreamHandle streamHandle = null;

	public OneForOneBlockFetcher(
			TransportClient client,
			String appId,
			String execId,
			String[] blockIds,
			BlockFetchingListener listener) {
		this.client = client;
		this.blockIds = blockIds;
		this.listener = listener;

		this.openMessage = new OpenBlock(appId, execId, blockIds);
		this.chunkCallback = new ChunkCallback();
	}

	/** 成功接收到一个chunk后的回调，这里chunk=block！ */
	private class ChunkCallback implements ChunkReceivedCallback {
		@Override
		public void onSuccess(int chunkIdx, ManagedBuffer buffer) {
			listener.onBlockFetchSuccess(blockIds[chunkIdx], buffer);
		}

		@Override
		public void onFailure(int chunkIdx, Throwable cause) {
			listener.onBlockFetchFailure(blockIds[chunkIdx], cause);
		}
	}

	/**
	 * 开始获取数据，每次每一chunk成功获取时会调用对应的监听器函数。
	 *
	 * 消息采用Java Serializer序列化，RPC响应结果为{@link StreamHandle}，这里会
	 * 立即发送所有获取chunk请求，不会限流。
	 *
	 */
	public void start() {
		Preconditions.checkArgument(blockIds.length > 0, "没有提供要去的block ids");

		client.sendRpcAsync(openMessage.toByteBuffer(), new RpcCallback() {
			@Override
			public void onSuccess(ByteBuffer response) {
				try {
					streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteByffer(response);
					log.debug("成功打开blocks：{}，准备开始获取chunks。", streamHandle);

					//立即请求所有的chunk
					for (int i = 0; i < streamHandle.numChunks; i++) {
						client.fetchChunk(streamHandle.streamId, i, chunkCallback);
					}
				} catch (Exception e) {
					log.error("在成功打开blocks后，开始获取chunks时失败：{}", e.getMessage());
					failRemainingBlocks(blockIds, e);
				}
			}

			@Override
			public void onFailure(Throwable cause) {
				log.error("打开blocks时失败：{}", cause.getMessage());
				failRemainingBlocks(blockIds, cause);
			}
		});
	}

	/**
	 * 针对每个blockId，调用监听器的失败函数通知用户获取失败
	 * @param cause
	 * @param cause
	 */
	private void failRemainingBlocks(String[] failedBlockIds, Throwable cause) {
		for (String blockId : failedBlockIds) {
			try {
				listener.onBlockFetchFailure(blockId, cause);
			} catch (Exception e) {
				log.error("调用onBlockFetchFailure失败：{}", e.getMessage());
			}
		}
	}
}
