package govind.incubator.buffer;

import com.google.common.base.Preconditions;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NettyManagedBuffer;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class TestManagedBuffer extends ManagedBuffer {
	final int len;
	NettyManagedBuffer underlying;

	public TestManagedBuffer(int len) {
		Preconditions.checkArgument(len < 128);
		this.len  =  len;
		byte[] bytes = new byte[len];
		for (int i = 0; i < len; i++) {
			bytes[i] = (byte) i;
		}

		underlying = new NettyManagedBuffer(Unpooled.wrappedBuffer(bytes));
	}

	@Override
	public long size() {
		return len;
	}

	@Override
	public ManagedBuffer retain() {
		underlying.retain();
		return this;
	}

	@Override
	public ManagedBuffer release() {
		underlying.release();
		return this;
	}

	@Override
	public InputStream createInputStream() throws IOException {
		return underlying.createInputStream();
	}

	@Override
	public Object nettyByteBuf() throws IOException {
		return underlying.nettyByteBuf();
	}

	@Override
	public ByteBuffer nioByteBuffer() throws IOException {
		return underlying.nioByteBuffer();
	}

	@Override
	public boolean equals(Object o) {
		if (o  instanceof ManagedBuffer) {
			try {
				ByteBuffer buffer = ((ManagedBuffer) o).nioByteBuffer();
				if (buffer.remaining() != len) {
					return false;
				} else {
					for (int i = 0; i < len; i++) {
						if (buffer.get(i) != i) {
							return false;
						}
					}
				}
				return true;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return underlying.hashCode();
	}
}
