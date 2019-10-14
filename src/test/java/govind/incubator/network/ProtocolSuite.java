package govind.incubator.network;

import govind.incubator.network.protocol.*;
import govind.incubator.network.protocol.codec.MessageDecoder;
import govind.incubator.network.protocol.codec.MessageEncoder;
import govind.incubator.network.util.NettyUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.apache.parquet.Ints;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.junit.Test;

import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-8
 */
public class ProtocolSuite {

	@Test
	public void request() {
		testClientToServer(new ChunkFetchRequest(new StreamChunkId(1, 2)));
		testClientToServer(new RpcRequest(12345, new TestManagedBuffer(0)));
		testClientToServer(new RpcRequest(12345, new TestManagedBuffer(10)));
		testClientToServer(new StreamRequest("abcde"));
		testClientToServer(new OneWayMessage(new TestManagedBuffer(10)));
	}

	@Test
	public void response() {
		testServerToClient(new ChunkFetchSuccess(new TestManagedBuffer(0), new StreamChunkId(1, 2)));
		testServerToClient(new ChunkFetchSuccess(new TestManagedBuffer(10), new StreamChunkId(2, 3)));
		testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2),"this is an error"));
		testServerToClient(new ChunkFetchFailure(new StreamChunkId(1, 2),""));

		testServerToClient(new RpcResponse(new TestManagedBuffer(0), 1234));
		testServerToClient(new RpcResponse(new TestManagedBuffer(10), 1234));
		testServerToClient(new RpcFailure(1234,""));
		testServerToClient(new RpcFailure(1234,"this is an error"));

		/**
		 * Note: buffer size must be "0" since StreamResponse's
		 * buffer is written differently to the  channel and cannot
		 * be tested like this.
		 */
		testServerToClient(new StreamResponse(new TestManagedBuffer(0),"abcde", 12345L));
		testServerToClient(new StreamFailure("abcde", ""));
		testServerToClient(new StreamFailure("abcde", "this is an error"));
	}

	private void testClientToServer(Message msg) {
		EmbeddedChannel clientChannel = new EmbeddedChannel(new FileRegionEncoder(), new MessageEncoder());
		clientChannel.writeOutbound(msg);

		EmbeddedChannel serverChannel = new EmbeddedChannel(NettyUtil.createFrameDecoder(), new MessageDecoder());
		while (!clientChannel.outboundMessages().isEmpty()) {
			serverChannel.writeInbound(clientChannel.readOutbound());
		}

		assertEquals(1, serverChannel.inboundMessages().size());
		assertEquals(msg, serverChannel.readInbound());
	}

	private void testServerToClient(Message msg) {
		EmbeddedChannel serverChannel = new EmbeddedChannel(new FileRegionEncoder(), new MessageEncoder());
		serverChannel.writeOutbound(msg);

		EmbeddedChannel clientChannel = new EmbeddedChannel(NettyUtil.createFrameDecoder(), new MessageDecoder());
		while (!serverChannel.outboundMessages().isEmpty()) {
			clientChannel.writeInbound(serverChannel.readOutbound());
		}

		assertEquals(1, clientChannel.inboundMessages().size());
		assertEquals(msg, clientChannel.readInbound());
	}

	/**
	 * 将FileRegion对象转换为字节缓冲区，由于EmbeddedChannel实际上传输的是Message对象
	 * 而不是字节流，因此需要在接收端添加一个解码器，以便能够知道MessageWithHeader中具体
	 * 包含的内容。
	 */
	private static class FileRegionEncoder extends MessageToMessageEncoder<FileRegion> {
		@Override
		protected void encode(ChannelHandlerContext ctx, FileRegion msg, List<Object> out) throws Exception {
			ByteArrayWritableChannel channel = new ByteArrayWritableChannel(Ints.checkedCast(msg.count()));

			while (msg.transfered() < msg.count()) {
				msg.transferTo(channel, msg.transfered());
			}
			out.add(Unpooled.wrappedBuffer(channel.getData()));
		}
	}
}
