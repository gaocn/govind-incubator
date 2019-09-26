package govind.incubator.network.protocol.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;

/**
 * 自定义FrameDecoder允许对原始数据进行拦截或截接(intercepting)
 * 1、Netty自带的Decoder每解析一条消息就会分发给handler进行处理，而
 * 自定义的Decoder允许保存尽可能多的buffer（直到OOM）并一次性分发所
 * 有数据。
 *
 * 2、允许child handler添加拦截器对数据进行处理，当拦截器存在时，则
 * 停止framing，并将缓存的数据发送给拦截器处理，当拦截器通知它不需要
 * 读取数据时，则继续进行framing。（这里的framing就是将接收到的数据
 * 先缓存到内部的List<ByteBuf>中）。
 *
 * 3、拦截器在处理data buffer时不能持有缓存数据的引用。
 */
@Slf4j
public class TransportFrameDecoder extends ChannelInboundHandlerAdapter {
	/** handler name */
	public static final String HANDLER_NAME = "frameDecoder";
	/** use long to store frame size */
	public static final int FRAME_LENGTH_SIZE = 8;
	/** maximum frame size */
	public static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;
	/** indicate invalid frame size */
	public static final int UNKNOWN_FRAME_SIZE = -1;

	/** data buffers */
	private final LinkedList<ByteBuf> buffers = new LinkedList<>();
	/** to store frame length in ByteBuf */
	private final ByteBuf frameLengthBuffer = Unpooled.buffer(FRAME_LENGTH_SIZE, FRAME_LENGTH_SIZE);


	/** statistics */
	/** total size in bytes */
	private long totalSize = 0;
	/** when decoder from {@link #buffers}, nextFrameSize indicate current frame size(not include frame length) to decode */
	private long nextFrameSize = UNKNOWN_FRAME_SIZE;

	/** if set, then every time {@link #channelRead(ChannelHandlerContext, Object)} is called, feed each decoded frame to Inteceptor. */
	private volatile Inteceptor inteceptor;


	/**
	 * 1、当缓存的数据不为空且存在拦截器时，则不停的将数据喂给拦截器，
	 * 直到拦截器停止消费或缓存数据为空。
	 * 2、当缓存数据不为空且不存在拦截器时，则从缓存区中取出一个frame
	 * 将其交给下一个handler处理。
	 * @param ctx
	 * @param msg
	 * @throws Exception
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof ByteBuf) {
			ByteBuf buf = (ByteBuf) msg;
			buffers.add(buf);
			totalSize += buf.readableBytes();

			while (!buffers.isEmpty()) {
				if (inteceptor == null) {
					ByteBuf frame = decodeNext();
					if (frame == null) {
						break;
					}
					log.debug("没有Intercepter，直接从缓存区中解析一条帧发送给下一个Handler处理。frame: {}", frame);
					ctx.fireChannelRead(frame);
				} else {
					log.debug("设置了拦截器，则将接收到的数据直接交给拦截器处理");
					ByteBuf first = buffers.getFirst();
					int available = first.readableBytes();

					if (feedInterceptor(first)) {
						assert !first.isReadable() : "inteceptor is alive, but buffer has uncomsumed data!";
					}

					/**
					 * 采用如下方式释放已读取数据的内容的问题：
					 * 当{@link Inteceptor}实例采用读取ByteBuf的方法无法修改{@link ByteBuf}的readIndex时
					 * 会导致ByteBuf对象一直被消费，不会被释放掉！
					 *
					 * 建议使用{@link ByteBuf.readSlice(int length)}方法进行消费
					 */
					int read = available - first.readableBytes();
					if (read == available) {
						buffers.removeFirst().release();
					}
					totalSize -= read;
				}
			}
		} else {
			throw new IllegalArgumentException("msg is not instance of ByteBuf!");
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		buffers.forEach(buf -> buf.release());
		if (inteceptor != null) {
			inteceptor.channelInactive();
		}
		frameLengthBuffer.release();
		super.channelInactive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (inteceptor != null) {
			inteceptor.exceptionCaught(cause);
		}
		super.exceptionCaught(ctx, cause);
	}

	/**
	 * 从{@link #buffers}中解析出一条帧，若没有完整的帧则返回空
	 * @return
	 */
	private ByteBuf decodeNext() {
		//1. 获取帧长度
		long frameSize = decodeFrameSize();
		assert frameSize < MAX_FRAME_SIZE : "帧长度过大：" + frameSize;
		assert  frameSize > 0 : "帧长度应该大于零";

		//2. 若当前缓存的数据量小于frameSize，则接收的帧不完整，继续接收
		if (frameSize == UNKNOWN_FRAME_SIZE || totalSize < frameSize) {
			return null;
		}

		//3. 已接收至少一个完整帧，可以进行解析，同时重置nextFrameSize为下一帧做准备
		nextFrameSize = UNKNOWN_FRAME_SIZE;
		int remaining = (int) frameSize;

		//4.1 若buffers中的一个缓存中包含全部frameSize个数据，则获取后直接返回
		if (buffers.getFirst().readableBytes() > remaining) {
			return nextBufferForFrame(remaining);
		}

		//4.2 若buffers中的一个缓存没有frameSize个数据，则需要组合多个buf
		CompositeByteBuf compositeBuf = buffers.getFirst().alloc().compositeBuffer();
		while (remaining > 0) {
			ByteBuf next = nextBufferForFrame(remaining);
			remaining -= next.readableBytes();
			compositeBuf.addComponent(next)
					.writerIndex(compositeBuf.writerIndex() + next.readableBytes());
		}
		assert remaining == 0 : "解析帧错误";
		return compositeBuf;
	}

	/**
	 * 从{@link #buffers}中解析帧长度，并将帧长度存放在{@link #nextFrameSize}中
	 * @return
	 */
	private long decodeFrameSize() {
		if (nextFrameSize != UNKNOWN_FRAME_SIZE || totalSize < FRAME_LENGTH_SIZE) {
			return nextFrameSize;
		}

		ByteBuf buf = buffers.getFirst();
		if (buf.readableBytes() > FRAME_LENGTH_SIZE) {
			nextFrameSize = buf.readLong() - FRAME_LENGTH_SIZE;
			totalSize -= FRAME_LENGTH_SIZE;
			if (!buf.isReadable()) {
				buffers.removeFirst().release();
			}
			return nextFrameSize;
		}

		while (frameLengthBuffer.readableBytes() < FRAME_LENGTH_SIZE) {
			ByteBuf next = buffers.getFirst();
			int toRead = Math.min(
					next.readableBytes(),
					FRAME_LENGTH_SIZE - frameLengthBuffer.readableBytes());

			frameLengthBuffer.writeBytes(next, toRead);
			if (!next.isReadable()) {
				buffers.removeFirst().release();
			}
		}

		nextFrameSize = frameLengthBuffer.readLong() - FRAME_LENGTH_SIZE;
		totalSize -= FRAME_LENGTH_SIZE;
		frameLengthBuffer.clear();
		return nextFrameSize;
	}

	/**
	 * 从{@link #buffers}中获取长度为bytesToRead的缓冲区，每次读取其中的一个缓存区进行解析！
	 * @param bytesToRead
	 * @return
	 */
	private ByteBuf nextBufferForFrame(int bytesToRead) {
		ByteBuf buf = buffers.getFirst();
		ByteBuf frameBuf = null;

		if (buf.readableBytes() > bytesToRead) {
			frameBuf = buf.retain().readSlice(bytesToRead);
			totalSize -= bytesToRead;
		} else {
			frameBuf = buf;
			buffers.removeFirst();
			totalSize -= frameBuf.readableBytes();
		}
		return frameBuf;
	}

	public void setInteceptor(Inteceptor inteceptor) {
		assert this.inteceptor == null : "不能重设拦截器";
		this.inteceptor = inteceptor;
	}

	/**
	 *
	 * @param buf
	 * @return
	 */
	private boolean feedInterceptor(ByteBuf buf) throws Exception {
		if (inteceptor != null && !inteceptor.handle(buf)) {
			return true;
		}
		return inteceptor != null;
	}
}
