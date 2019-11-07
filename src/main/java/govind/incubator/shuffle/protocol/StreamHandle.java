package govind.incubator.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * @Author: 高文文
 * Project Name: govind-incubator
 * Date: 2019-11-1
 *
 * {@link OpenBlock}消息的响应消息，返回从某个Stream可以读取的chunk数。
 * 在OneForOneBlockFetcher中使用。
 *
 */
public class StreamHandle extends BlockTransferMessage {
	public final long streamId;
	public final int numChunks;

	public StreamHandle(long streamId, int numChunks) {
		this.streamId = streamId;
		this.numChunks = numChunks;
	}

	@Override
	protected Type type() {
		return Type.STREAM_HANDLE;
	}

	@Override
	public int encodedLength() {
		return 8 + 4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(streamId);
		buf.writeInt(numChunks);
	}

	public static StreamHandle decode(ByteBuf buf) {
		long streamId = buf.readLong();
		int numChunks = buf.readInt();
		return new StreamHandle(streamId, numChunks);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamId, numChunks);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof StreamHandle) {
			StreamHandle sh = (StreamHandle)obj;
			return Objects.equal(streamId, sh.streamId)
					&& Objects.equal(numChunks, sh.numChunks);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamId", streamId)
				.add("numChunks", numChunks)
				.toString();
	}
}
