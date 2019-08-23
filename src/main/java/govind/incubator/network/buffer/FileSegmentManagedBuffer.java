package govind.incubator.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class FileSegmentManagedBuffer extends ManagedBuffer{

	@Override
	public long size() {
		return 0;
	}

	@Override
	public ManagedBuffer retail() {
		return null;
	}

	@Override
	public ManagedBuffer release() {
		return null;
	}

	@Override
	public InputStream createInputStream() throws IOException {
		return null;
	}

	@Override
	public Object nettyByteBuf() throws IOException {
		return null;
	}

	@Override
	public ByteBuffer nioByteBuffer() throws IOException {
		return null;
	}
}
