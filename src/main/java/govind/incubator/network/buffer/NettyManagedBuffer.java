package govind.incubator.network.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * 基于ByteBuf的容器的直接字节数据 */
public class NettyManagedBuffer extends ManagedBuffer{

	final ByteBuf buf;

	public NettyManagedBuffer(ByteBuf buf) {
		this.buf = buf;
	}

	@Override
	public long size() {
		return buf.readableBytes();
	}

	@Override
	public ManagedBuffer retain() {
		buf.retain();
		return this;
	}

	@Override
	public ManagedBuffer release() {
		buf.release();
		return this;
	}

	@Override
	public InputStream createInputStream() throws IOException {
		return new ByteBufInputStream(buf);
	}

	/**
	 * 返回基于当前buf创建的ByteBuf对象，两者共享内容，但具有独立的limit、
	 * position等
	 * @return
	 * @throws IOException
	 */
	@Override
	public Object nettyByteBuf() throws IOException {
		return buf.duplicate();
	}

	/**
	 * 返回具有独立limit、position、mark的ByteBuffer对象，但两者共享
	 * 其中的内容！
	 * @return
	 * @throws IOException
	 */
	@Override
	public ByteBuffer nioByteBuffer() throws IOException {
		return buf.nioBuffer();
	}

	@Override
	public String toString() {
		return "NettyManagedBuffer{" +
				"buf=" + buf +
				'}';
	}

	//TODO 验证正确性
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}
}
