package govind.incubator.network.buffer;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 用于NIO ByteBuffer表示的字节数据
 */
public class NioManagedBuffer extends ManagedBuffer{

	final ByteBuffer buffer;

	public NioManagedBuffer(ByteBuffer buffer) {
		this.buffer = buffer;
	}

	/**
	 * 返回limit-position
	 * @return
	 */
	@Override
	public long size() {
		return buffer.remaining();
	}

	/**
	 * ByteBuffer不需要进行引用计数
	 * @return
	 */
	@Override
	public ManagedBuffer retain() {
		return this;
	}

	/**
	 * ByteBuffer不需要进行引用计数
	 * @return
	 */
	@Override
	public ManagedBuffer release() {
		return this;
	}

	/**
	 * ByteBufInputStream根据ByteBuf中的readIndex、writeIndex创建
	 * InputStream对象！
	 * @return
	 * @throws IOException
	 */
	@Override
	public InputStream createInputStream() throws IOException {
		return new ByteBufInputStream(Unpooled.wrappedBuffer(buffer));
	}

	/**
	 * 返回基于当前NIO ByteBuffer创建的ByteBuf对象，两者共享内容，但具
	 * 有独立的limit、position等
	 * @return
	 * @throws IOException
	 */
	@Override
	public Object nettyByteBuf() throws IOException {
		return Unpooled.wrappedBuffer(this.buffer);
	}

	/**
	 * 返回具有独立limit、position、mark的ByteBuffer对象，但两者共享
	 * 其中的内容！
	 * @return
	 * @throws IOException
	 */
	@Override
	public ByteBuffer nioByteBuffer() throws IOException {
		return buffer.duplicate();
	}

	@Override
	public String toString() {
		return "NioManagedBuffer{" +
				"buffer=" + buffer +
				'}';
	}
}
