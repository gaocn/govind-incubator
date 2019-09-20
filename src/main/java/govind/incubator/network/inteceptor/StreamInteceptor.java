package govind.incubator.network.inteceptor;


import govind.incubator.network.handler.StreamCallback;
import govind.incubator.network.handler.TransportResponseHandler;
import govind.incubator.network.protocol.codec.Inteceptor;
import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-9-20
 * <p>
 * 流处理器，在{@link govind.incubator.network.protocol.codec.TransportFrameDecoder}中注册，
 * 用于消费其中的数据，并调用{@link govind.incubator.network.handler.StreamCallback}中的方法
 * 进行消费
 */
@Slf4j
public class StreamInteceptor implements Inteceptor {
	/**
	 * 该流处理器关联的响应处理器
	 */
	private final TransportResponseHandler handler;

	/**
	 * 该流处理器所处理的流的标识
	 */
	private final String streamId;

	/**
	 * 该流处理器需要处理的字节数
	 */
	private final long byteCount;

	/**
	 * 消费流中数据的回调函数
	 */
	private final StreamCallback streamCallback;

	/**
	 * 记录已消费的字节数
	 */
	private volatile long byteRead;

	public StreamInteceptor(TransportResponseHandler handler, String streamId, long byteCount, StreamCallback streamCallback) {
		this.handler = handler;
		this.streamId = streamId;
		this.byteCount = byteCount;
		this.streamCallback = streamCallback;
		this.byteRead = 0;
	}

	@Override
	public boolean handle(ByteBuf buf) throws Exception {
		int toRead = (int) Math.min(buf.readableBytes(), byteCount - byteRead);
		ByteBuffer nioBuffer = buf.readSlice(toRead).nioBuffer();

		int available = nioBuffer.remaining();

		streamCallback.onData(streamId, nioBuffer);
		byteRead += available;

		if (byteRead > byteCount) {
			new IllegalStateException(String.format("消费过多数据？期待消费 %d 字节，实际消费 %d 字节",byteCount, byteRead));
		} else if (byteRead == byteCount) {
			handler.deactiveStream();
			streamCallback.onComplete(streamId);
		}
		return byteRead != byteCount;
	}

	@Override
	public void channelInactive() throws Exception {
		try {
			handler.deactiveStream();
			streamCallback.onFailure(streamId, new ClosedChannelException());
		} catch (IOException e) {
			log.error("调用StreamCall#onFailure方法出错：{}", e.getMessage());
		}
	}

	@Override
	public void exceptionCaught(Throwable cause) throws Exception {
		handler.deactiveStream();
		streamCallback.onFailure(streamId, cause);
	}
}
