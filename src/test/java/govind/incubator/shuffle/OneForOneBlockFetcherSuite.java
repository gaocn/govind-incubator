package govind.incubator.shuffle;

import com.google.common.collect.Maps;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.shuffle.protocol.BlockTransferMessage;
import govind.incubator.shuffle.protocol.BlockTransferMessage.Decoder;
import govind.incubator.shuffle.protocol.OpenBlock;
import govind.incubator.shuffle.protocol.StreamHandle;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-6
 */
public class OneForOneBlockFetcherSuite {

	@Test
	public void testFetchOne() {
		LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
		blocks.put("shuffle_0_0_0", new NioManagedBuffer(ByteBuffer.wrap(new byte[0])));

		BlockFetchingListener listener = fetchBlocks(blocks);
		verify(listener).onBlockFetchSuccess("shuffle_0_0_0", blocks.get("shuffle_0_0_0"));
	}

	@Test
	public void  testFetchThree() {
		LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
		blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
		blocks.put("b1", new NioManagedBuffer(ByteBuffer.wrap(new byte[34])));
		blocks.put("b2", new NioManagedBuffer(ByteBuffer.wrap(new byte[56])));

		BlockFetchingListener listener = fetchBlocks(blocks);

		for (int i = 0; i < blocks.size(); i++) {
			verify(listener).onBlockFetchSuccess("b" + i, blocks.get("b" + i));
		}
	}

	@Test
	public void testFailure() {
		LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
		blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
		blocks.put("b1", null);
		blocks.put("b2", null);

		BlockFetchingListener listener = fetchBlocks(blocks);

		verify(listener, times(1)).onBlockFetchSuccess("b0", blocks.get("b0"));
		verify(listener, times(1)).onBlockFetchFailure(eq("b1"), any());
		verify(listener, times(1)).onBlockFetchFailure(eq("b2"), any());

	}

	@Test
	public void testSuccessAndFailure() {
		LinkedHashMap<String, ManagedBuffer> blocks = Maps.newLinkedHashMap();
		blocks.put("b0", new NioManagedBuffer(ByteBuffer.wrap(new byte[12])));
		blocks.put("b1", null);
		blocks.put("b2", new NioManagedBuffer(ByteBuffer.wrap(new byte[34])));

		BlockFetchingListener listener = fetchBlocks(blocks);

		verify(listener, times(1)).onBlockFetchSuccess("b0", blocks.get("b0"));
		verify(listener, times(1)).onBlockFetchFailure(eq("b1"), any());
		verify(listener, times(1)).onBlockFetchSuccess("b2", blocks.get("b2"));
	}

	/**
	 * 1、通过mock server端，响应结果为<blockId, block>；
	 * 2、采用LinkedHashMap保证响应顺序与请求顺序一致；
	 * 3、若block为空或不存在则抛出异常；
	 */
	private BlockFetchingListener fetchBlocks(final LinkedHashMap<String, ManagedBuffer> blocks) {
		TransportClient client = mock(TransportClient.class);
		BlockFetchingListener listener = mock(BlockFetchingListener.class);
		String[] blockIds = blocks.keySet().toArray(new String[blocks.size()]);
		OneForOneBlockFetcher blockFetcher = new OneForOneBlockFetcher(client, "app-0", "exec-0", blockIds, listener);

		doAnswer((Answer<Void>) invocation -> {
			BlockTransferMessage msg = Decoder.fromByteByffer(
					(ByteBuffer) invocation.getArguments()[0]
			);
			RpcCallback callback = (RpcCallback) invocation.getArguments()[1];
			callback.onSuccess(new StreamHandle(123, blocks.size()).toByteBuffer());

			assertEquals(new OpenBlock("app-0", "exec-0", blockIds), msg);
			return null;
		}).
		when(client).sendRpcAsync(any(ByteBuffer.class), any(RpcCallback.class));


		//针对每个block请求返回对应的ManagedBuffer值
		final AtomicInteger expectedChunkIdx = new AtomicInteger(0);
		final Iterator<ManagedBuffer> blockIter = blocks.values().iterator();

		doAnswer((Answer<Void>) invocation -> {
			try {
				long streamId = (long) invocation.getArguments()[0];
				int chunkIdx = (int) invocation.getArguments()[1];
				assertEquals(123, streamId);
				assertEquals(expectedChunkIdx.getAndIncrement(), chunkIdx);

				ChunkReceivedCallback callback = (ChunkReceivedCallback) invocation.getArguments()[2];
				ManagedBuffer buffer = blockIter.next();
				if (buffer != null) {
					callback.onSuccess(chunkIdx, buffer);
				} else {
					callback.onFailure(chunkIdx, new RuntimeException("未找到block=" + chunkIdx));
				}
			} catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
			return null;
		}).
		when(client).fetchChunk(anyLong(), anyInt(),any(ChunkReceivedCallback.class));

		blockFetcher.start();
		return listener;
	}
}
