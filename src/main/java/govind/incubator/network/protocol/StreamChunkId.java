package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

/**
 * 对某个Stream中某个Chunk数据块标识的封装
 */
public class StreamChunkId implements Encodable {
	public final long  streamId;
	public final int  chunkIdx;

	public StreamChunkId(long streamId, int chunkIdx) {
		this.streamId = streamId;
		this.chunkIdx = chunkIdx;
	}

	@Override
	public int encodedLength() {
		return 8 + 4;
	}

	@Override
	public void encode(ByteBuf buf) {
		buf.writeLong(streamId);
		buf.writeInt(chunkIdx);
	}

	public static StreamChunkId decode(ByteBuf buf) {
		assert buf.readableBytes() >= 8 + 4 : "数据不完整，无法解析StreamChunkId";
		long streamId = buf.readLong();
		int chunkIdx = buf.readInt();
		return new StreamChunkId(streamId, chunkIdx);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StreamChunkId) {
			StreamChunkId o = (StreamChunkId)obj;
			return streamId == o.streamId  && chunkIdx == o.chunkIdx;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamId, chunkIdx);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamId", streamId)
				.add("chunkIdx", chunkIdx)
				.toString();
	}
}
