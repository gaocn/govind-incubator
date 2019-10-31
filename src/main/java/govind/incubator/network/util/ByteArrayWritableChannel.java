package govind.incubator.network.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-10-30
 *
 * A writable channel that stores the written data in a byte
 * array in memory.
 *
 */
public class ByteArrayWritableChannel implements WritableByteChannel {
	private final byte[] data;
	private int  offset;

	public ByteArrayWritableChannel(int size) {
		this.data = new byte[size];
	}

	public byte[] getData() {
		return data;
	}

	public int length() {
		return offset;
	}

	/**
	 * Resets the channel so that writing to it will overwrite
	 * the existing buffer.
	 */
	public void reset() {
		offset = 0;
	}

	/**
	 * Reads from the given buffer into the internal byte array.
	 * @param src
	 * @return
	 * @throws IOException
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		int toTransfer = Math.min(src.remaining(), data.length - offset);
		src.get(data, offset, toTransfer);
		offset += toTransfer;
		return toTransfer;
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void close() throws IOException {

	}
}
