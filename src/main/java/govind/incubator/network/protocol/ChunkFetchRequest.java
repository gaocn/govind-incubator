package govind.incubator.network.protocol;

import io.netty.buffer.ByteBuf;

import java.util.Objects;

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
	public int hashCode() {
		return Objects.hashCode(this);
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
