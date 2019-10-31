package govind.incubator.network.sasl;

import com.google.common.base.Preconditions;
import govind.incubator.network.util.ByteArrayWritableChannel;
import govind.incubator.network.util.NettyUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.util.AbstractReferenceCounted;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 *
 * Provides SASL-based encription for transport channels. The single method
 * exposed by this class installs the needed channel handlers on a connected
 * channel.
 *
 */
public class SaslEncryption {

	public static final String ENCRYPTION_HANDLER_NAME = "saslEncryption";

	/**
	 * Adds channel handlers that perform encryption / decryption of data using SASL.
	 *
	 * @param channel The channel
	 * @param backend The SASL backend
	 * @param maxOutboundBlockSize Max size in bytes of outgoing encrypted
	 *                                blocks, to control memory usage.
	 */
	public static void addToChannel(
			Channel channel,
			SaslEncryptionBackend backend,
			int maxOutboundBlockSize) {

		channel.pipeline()
				.addFirst(ENCRYPTION_HANDLER_NAME, new EncryptionHandler(backend, maxOutboundBlockSize))
				.addFirst("saslDecryption", new DecryptionHandler(backend))
				.addFirst("saslFrameDecoder", NettyUtil.createFrameDecoder());
	}


	@Slf4j
	private static class EncryptionHandler extends ChannelOutboundHandlerAdapter {
		private final int maxOutboundBlockSize;
		private final SaslEncryptionBackend backend;

		public EncryptionHandler(SaslEncryptionBackend backend,int maxOutboundBlockSize) {
			this.maxOutboundBlockSize = maxOutboundBlockSize;
			this.backend = backend;
		}

		@Override
		public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
			EncryptedMessage encryptedMessage = new EncryptedMessage(backend, msg, maxOutboundBlockSize);
			log.debug("加密消息：{}", encryptedMessage);
			ctx.write(encryptedMessage, promise);
		}

		@Override
		public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
			try {
				backend.dispose();
			} finally {
				super.handlerRemoved(ctx);
			}
		}
	}

	@Slf4j
	private static class DecryptionHandler  extends MessageToMessageDecoder<ByteBuf> {

		private final SaslEncryptionBackend backend;

		public DecryptionHandler(SaslEncryptionBackend backend) {
			this.backend = backend;
		}

		@Override
		protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
			byte[] data;
			int offset;
			int length = msg.readableBytes();

			if (msg.hasArray()) {
				data = msg.array();
				offset = msg.arrayOffset();
				msg.skipBytes(length);
			} else {
				data = new byte[length];
				msg.readBytes(data);
				offset = 0;
			}
			ByteBuf decodedBuf = Unpooled.wrappedBuffer(backend.unwrap(data, offset, length));
			log.debug("解密消息：{}", decodedBuf);
			out.add(decodedBuf);
		}
	}

	public static class EncryptedMessage extends AbstractReferenceCounted implements FileRegion {

		private final SaslEncryptionBackend backend;
		private final boolean isByteBuf;
		private final ByteBuf buf;
		private final FileRegion region;

		/**
		 *  A channel used to buffer input data for encryption. The channel
		 *  has an upper size bound so that if the input is larger than the
		 *  allowed buffer, it will be broken into multiple chunks.
		 */
		private final ByteArrayWritableChannel byteChannel;

		private ByteBuf currentHeader;
		private ByteBuffer currentChunk;
		private long currentChunkSize;
		private long currentReportedBytes;
		private long unencryptedChunkSize;
		private long transferred;

		public EncryptedMessage(SaslEncryptionBackend backend,
								Object msg,
								int maxOutboundBlockSize) {
			Preconditions.checkArgument(msg instanceof ByteBuf || msg instanceof FileRegion, "非法消息类型：%s", msg.getClass().getName());
			this.backend = backend;
			this.isByteBuf = msg instanceof ByteBuf;
			this.buf = isByteBuf ? (ByteBuf) msg : null;
			this.region  = isByteBuf ? null : (FileRegion)msg;
			this.byteChannel = new ByteArrayWritableChannel(maxOutboundBlockSize);
		}

		@Override
		public long position() {
			return 0;
		}

		/**
		 * 返回已传输数据的大致数量，参看{@link #count()}
		 */
		@Override
		public long transfered() {
			return transferred;
		}

		/**
		 * 返回未被加密时的消息长度
		 *
		 * 因为无法提前获得消息加密后的大小，这里对Netty如何处理FileRegion
		 * 实例做出如下假设：当<code>transfered() < count()</code>时，
		 * Netty会尝试不断地将消息发送出去，因此实际上这两个方法的返回值都是
		 * “不准确”的。
		 *
		 */
		@Override
		public long count() {
			return isByteBuf ? buf.readableBytes() : region.count();
		}

		/**
		 * Transfers data from the original message to the channel, encrypting
		 * it in the process.
		 *
		 * This method also breaks down the original message into smaller chunks
		 * when needed. This is done to keep memory usage under control. This avoids
		 * having to copy the whole message data into memory at once, and can avoid
		 * ballooning memory usage when transferring large messages such as shuffle blocks.
		 *
		 * The {@link #transfered()} counter also behaves a little funny, in that it
		 * won't go forward until a whole chunk has been written. This is done because
		 * the code can't use the actual number of bytes written to the channel as the
		 * transferred count (see {@link #count()}). Instead, once an encrypted chunk
		 * is written to the output (including its header), the size of the original
		 * block will be added to the {@link #transfered()} amount.
		 */
		@Override
		public long transferTo(WritableByteChannel target, long position) throws IOException {
			Preconditions.checkArgument(position == transfered(), "非法position：%s", position);

			long reportedWritten = 0L;
			long actuallyWritten = 0L;

			do {

				if (currentChunk == null) {
					nextChunk();
				}

				//1. 发送头部信息
				if (currentHeader.readableBytes()  > 0) {
					int bytesWritten = target.write(currentHeader.nioBuffer());
					currentHeader.skipBytes(bytesWritten);
					actuallyWritten += bytesWritten;
					if (currentHeader.readableBytes() > 0) {
						//Break out of loop if there are still header bytes left to write.
						break;
					}
				}

				//2. 发送body中的内容，若需要可能得分批发送
				actuallyWritten += target.write(currentChunk);
				if (!currentChunk.hasRemaining()) {
					//一块chunk已经写入成功，则更新记录已经发送数据，为发送下一个chunk准备
					long chunkBytesRemaining = unencryptedChunkSize - currentReportedBytes;
					reportedWritten += chunkBytesRemaining;
					transferred += chunkBytesRemaining;
					currentHeader.release();
					currentHeader = null;
					currentChunk = null;
					currentChunkSize = 0;
					currentReportedBytes = 0;
				}
			} while (currentChunk == null && transfered() + reportedWritten < count());

			//Returning 0 triggers a backoff mechanism in netty which may harm performance.
			// Instead, we return 1 until we can (i.e. until the reported count would
			// actually match the size  of the current chunk), at which point we resort to
			// returning 0 so that the counts still  match, at the cost of some performance.
			// That situation should be rare, though.
			if (reportedWritten != 0L) {
				return reportedWritten;
			}

			//optimal code to avoid triggering backoff mechanism in netty!!
			if (actuallyWritten > 0 && currentReportedBytes < currentChunkSize - 1) {
				transferred += 1L;
				currentReportedBytes += 1L;
				return 1L;
			}
			return 0L;
		}

		private void nextChunk() throws IOException {
			byteChannel.reset();
			if (isByteBuf) {
				int copied = byteChannel.write(buf.nioBuffer());
				buf.skipBytes(copied);
			} else {
				region.transferTo(byteChannel, region.transfered());
			}

			byte[] encrypted = backend.wrap(byteChannel.getData(), 0, byteChannel.length());

			this.currentChunk = ByteBuffer.wrap(encrypted);
			this.currentChunkSize = encrypted.length;
			this.currentHeader = Unpooled.copyLong(8 + currentChunkSize);
			this.unencryptedChunkSize = byteChannel.length();
		}

		@Override
		protected void deallocate() {
			if (currentHeader != null) {
				currentHeader.release();
			}

			if (buf != null) {
				buf.release();
			}

			if (region != null) {
				region.release();
			}
		}
	}
}
