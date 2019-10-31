package govind.incubator.network;

import govind.incubator.buffer.TestManagedBuffer;
import govind.incubator.network.buffer.NioManagedBuffer;
import govind.incubator.network.handler.ChunkReceivedCallback;
import govind.incubator.network.handler.RpcCallback;
import govind.incubator.network.handler.StreamCallback;
import govind.incubator.network.handler.TransportResponseHandler;
import govind.incubator.network.protocol.*;
import govind.incubator.network.protocol.codec.TransportFrameDecoder;
import io.netty.channel.local.LocalChannel;
import org.junit.Test;

import java.nio.ByteBuffer;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-21
 */
public class TransportResponseHandlerSuite {
	@Test
	public void handlerSuccessFulFetch() throws Exception {
		StreamChunkId streamChunkId = new StreamChunkId(1, 0);

		TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
		ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
		handler.addFetchRequest(streamChunkId, callback);
		assertEquals(1, handler.numOfOutstandingRequests());

		handler.handler(new ChunkFetchSuccess(new TestManagedBuffer(123), streamChunkId));
		verify(callback, times(1)).onSuccess(eq(0), any());
		assertEquals(0, handler.numOfOutstandingRequests());
	}

	@Test
	public void handleFailFetch() throws Exception {
		StreamChunkId streamChunkId = new StreamChunkId(1, 0);

		TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
		ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
		handler.addFetchRequest(streamChunkId, callback);
		assertEquals(1, handler.numOfOutstandingRequests());

		handler.handler(new ChunkFetchFailure(streamChunkId, "error msg"));
		verify(callback, times(1)).onFailure(eq(streamChunkId.chunkIdx), any());
		assertEquals(0, handler.numOfOutstandingRequests());
	}

	@Test
	public void clearAllOutstandingRequests() throws Exception {
		TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
		ChunkReceivedCallback callback = mock(ChunkReceivedCallback.class);
		handler.addFetchRequest(new StreamChunkId(1, 0), callback);
		handler.addFetchRequest(new StreamChunkId(1, 1), callback);
		handler.addFetchRequest(new StreamChunkId(1, 2), callback);
		assertEquals(3, handler.numOfOutstandingRequests());

		handler.handler(new ChunkFetchSuccess(new TestManagedBuffer(123),new StreamChunkId(1,0)));
		handler.exceptionCaught(new Exception("tt tt ttt"));

		//第2和3个请求应该失败
		verify(callback, times(1)).onSuccess(eq(0), any());
		verify(callback, times(1)).onFailure(eq(1), any());
		verify(callback, times(1)).onFailure(eq(2), any());
		assertEquals(0, handler.numOfOutstandingRequests());
	}

	@Test
	public void handlerRpcResponse() throws Exception {
		TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
		RpcCallback callback = mock(RpcCallback.class);

		handler.addRpcRequest(0, callback);
		assertEquals(1, handler.numOfOutstandingRequests());

		handler.handler(new RpcResponse(new TestManagedBuffer(123), 0));
		verify(callback, times(1)).onSuccess(any());
		assertEquals(0, handler.numOfOutstandingRequests());

		handler.addRpcRequest(123, callback);
		ByteBuffer buffer = ByteBuffer.allocate(10);
		handler.handler(new RpcResponse(new NioManagedBuffer(buffer), 123));

		verify(callback,  times(1)).onSuccess(eq(ByteBuffer.allocate(10)));
		assertEquals(0, handler.numOfOutstandingRequests());
	}

	@Test
	public void testRpcFail() throws Exception {
		TransportResponseHandler handler = new TransportResponseHandler(new LocalChannel());
		RpcCallback callback = mock(RpcCallback.class);

		handler.addRpcRequest(456, callback);
		handler.addRpcRequest(789, callback);
		assertEquals(2, handler.numOfOutstandingRequests());

		handler.handler(new RpcFailure(456, "error msg"));
		verify(callback, times(1)).onFailure(any());

		handler.handler(new RpcFailure(789, "error msg oh"));
		assertEquals(0, handler.numOfOutstandingRequests());


	}


	@Test
	public void testActiveStream() throws Exception {
		LocalChannel channel = new LocalChannel();
		channel.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder());
		TransportResponseHandler handler = new TransportResponseHandler(channel);
		StreamCallback callback = mock(StreamCallback.class);

		StreamResponse response = new StreamResponse(null, "123", 1234);

		handler.addStreamRequest(callback);
		assertEquals(1, handler.numOfOutstandingRequests());
		handler.handler(response);
		assertEquals(1, handler.numOfOutstandingRequests());

		handler.deactiveStream();
		assertEquals(0, handler.numOfOutstandingRequests());

		StreamFailure failure = new StreamFailure("fialedStream", "some error msg");
		handler.addStreamRequest(callback);
		assertEquals(1, handler.numOfOutstandingRequests());
		handler.handler(failure);
		assertEquals(0, handler.numOfOutstandingRequests());
	}
}
