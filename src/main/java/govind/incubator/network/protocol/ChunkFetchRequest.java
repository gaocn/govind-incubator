package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

public class ChunkFetchRequest extends AbstractMessage implements RequestMessage{
	public final StreamChunkId streamChunkId;

	public ChunkFetchRequest(StreamChunkId streamChunkId) {
		this.streamChunkId = streamChunkId;
	}

	@Override
	public Type type() {
		return Type.ChunkFetchRequest;
	}

	@Override
	public int encodedLength() {
		return streamChunkId.encodedLength();
	}

	@Override
	public void encode(ByteBuf buf) {
		streamChunkId.encode(buf);
	}

	public static ChunkFetchRequest decode(ByteBuf buf) {
		StreamChunkId streamChunkId = StreamChunkId.decode(buf);
		return new ChunkFetchRequest(streamChunkId);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamChunkId", streamChunkId)
				.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamChunkId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ChunkFetchRequest) {
			ChunkFetchRequest o = (ChunkFetchRequest)obj;
			return streamChunkId.equals(o.streamChunkId);

		}
		return false;
	}
}
