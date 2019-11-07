package govind.incubator.shuffle;

import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.client.TransportClient;
import govind.incubator.network.handler.OneForOneStreamManager;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.RpcHandler;
import govind.incubator.shuffle.protocol.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.nio.ByteBuffer;
import java.util.Iterator;

import static junit.framework.TestCase.*;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-4
 */
public class ExternalShuffleBlockHandlerSuite {
	TransportClient client = mock(TransportClient.class);

	OneForOneStreamManager streamManager;
	ExternalShuffleBlockResolver blockManager;
	RpcHandler  handler;

	@Before
	public void beforeEach() {
		streamManager = mock(OneForOneStreamManager.class);
		blockManager = mock(ExternalShuffleBlockResolver.class);
		handler = new ExternalShuffleBlockHandler(blockManager, streamManager);
	}

	@Test
	public void testRegisterExecutor() {
		RpcCallback callback = mock(RpcCallback.class);

		ExecutorShuffleInfo shuffleInfo = new ExecutorShuffleInfo(new String[]{"/a", "/b"}, 16, "sort");
		RegisterExecutor registerExecutor = new RegisterExecutor("app0", "exec1", shuffleInfo);
		handler.receive(client, registerExecutor.toByteBuffer(), callback);
		verify(blockManager, times(1)).registerExecutor("app0", "exec1",shuffleInfo);

		verify(callback, times(1)).onSuccess(any(ByteBuffer.class));
		verify(callback, never()).onFailure(any());
	}

	@Test
	public void testOpenBlock() {
		RpcCallback callback = mock(RpcCallback.class);

		NioManagedBuffer buffer1 = new NioManagedBuffer(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
		NioManagedBuffer buffer2 = new NioManagedBuffer(ByteBuffer.wrap(new byte[]{5, 6, 7}));
		when(blockManager.getBlockData("app0", "exec1", "b1"))
				.thenReturn(buffer1);
		when(blockManager.getBlockData("app0",  "exec1", "b2"))
				.thenReturn(buffer2);

		OpenBlock openBlock = new OpenBlock("app0", "exec1", new String[]{"b1", "b2"});
		handler.receive(client, openBlock.toByteBuffer(),callback);

		verify(blockManager, times(1)).getBlockData("app0",  "exec1", "b1");
		verify(blockManager, times(1)).getBlockData("app0",  "exec1", "b2");

		ArgumentCaptor<ByteBuffer> response = ArgumentCaptor.forClass(ByteBuffer.class);
		verify(callback, times(1)).onSuccess(response.capture());
		verify(callback, never()).onFailure(any());

		StreamHandle streamHandle = (StreamHandle) BlockTransferMessage.Decoder.fromByteByffer(response.getValue());
		assertEquals(2, streamHandle.numChunks);

		ArgumentCaptor<Iterator<ManagedBuffer>> stream = ArgumentCaptor.forClass(Iterator.class);

		verify(streamManager, times(1)).registerStream(any(), stream.capture());

		Iterator<ManagedBuffer> managedBuffers = stream.getValue();
		assertEquals(buffer1, managedBuffers.next());
		assertEquals(buffer2, managedBuffers.next());
		assertFalse(managedBuffers.hasNext());
	}

	@Test
	public void testBadMessage() {
		RpcCallback callback = mock(RpcCallback.class);

		ByteBuffer buffer = ByteBuffer.wrap(new byte[]{0x12, 0x34, 0x56});

		try {
			handler.receive(client, buffer,callback);
			fail("应该会抛出异常");
		} catch (Exception e) {
			//pass
		}

		ByteBuffer buffer1 = new UploadBlock("a", "b", "c", new byte[]{0, 1}, new byte[]{2, 3}).toByteBuffer();

		try {
			handler.receive(client, buffer1, callback);
			fail("应该抛出异常，不支持处理该类型的消息");
		} catch (Exception e) {
			//pass
		}

		verify(callback, never()).onSuccess(any());
		verify(callback, never()).onFailure(any());
	}
}
