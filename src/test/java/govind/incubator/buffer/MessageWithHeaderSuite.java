package govind.incubator.buffer;

import govind.incubator.network.protocol.MessageWithHeader;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import static junit.framework.TestCase.*;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-26
 *
 * MessageWithHeader单元测试
 *
 */
public class MessageWithHeaderSuite {


	@Test
	public void testByteBufBody() throws IOException {
		ByteBuf header = Unpooled.copyLong(42);
		ByteBuf body = Unpooled.copyLong(84);
		MessageWithHeader msg = new MessageWithHeader(header, body, body.readableBytes());

		ByteBuf result = doWrite(msg, 1);
		assertEquals(msg.count(), result.readableBytes());
		assertEquals(42, result.readLong());
		assertEquals(84, result.readLong());
	}

	@Test
	public void testSingleWrite() throws IOException {
		testFileRegionBody(8, 8);
	}

	@Test
	public void testShortWrite() throws IOException {
		testFileRegionBody(8, 1);
	}


	private void testFileRegionBody(int totalWrites, int writesPerCall) throws IOException {
		ByteBuf header = Unpooled.copyLong(42);
		int headerLength = header.readableBytes();

		TestFileRegion region = new TestFileRegion(totalWrites, writesPerCall);

		MessageWithHeader message = new MessageWithHeader(header, region, region.count());

		ByteBuf result = doWrite(message, totalWrites / writesPerCall);

		assertEquals(headerLength + region.count(), result.readableBytes());
		assertEquals(42, result.readLong());

		for (long i = 0; i < 8; i++) {
			assertEquals(i, result.readLong());
		}
	}

	private ByteBuf doWrite(MessageWithHeader msg, int minExpectedWrites) throws IOException {
		int writes = 0;
		ByteArrayWritableChannel channel = new ByteArrayWritableChannel((int) msg.count());

		while (msg.transfered() < msg.count()) {
			msg.transferTo(channel, msg.transfered());
			writes++;
		}
		assertTrue("Not Enough writes!", minExpectedWrites <= writes);
		return Unpooled.wrappedBuffer(channel.getData());
	}

	/**
	 * 每次写入以8字节为单位
	 */
	private static class TestFileRegion extends AbstractReferenceCounted implements FileRegion {
		private final int writeCount;
		private final int writesPerCall;

		private int written;

		public TestFileRegion(int writeCount, int writesPerCall) {
			this.writeCount = writeCount;
			this.writesPerCall = writesPerCall;
		}

		@Override
		public long position() {
			return 0;
		}

		@Override
		public long transfered() {
			return written * 8;
		}

		@Override
		public long count() {
			return writeCount * 8;
		}

		@Override
		public long transferTo(WritableByteChannel target, long position) throws IOException {
			for (int i = 0; i < writesPerCall; i++) {
				ByteBuf buf = Unpooled.copyLong((position / 8) + i);
				ByteBuffer nioBuffer = buf.nioBuffer();

				while (nioBuffer.remaining() > 0) {
					target.write(nioBuffer);
				}
				buf.release();
				written++;
			}
			return 8 * writesPerCall;
		}

		@Override
		protected void deallocate() {}
	}
}
