package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import govind.incubator.network.util.CodecUtil.Strings;
import io.netty.buffer.ByteBuf;

public class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {

	public final StreamChunkId streamChunkId;
	public final String error;

	public ChunkFetchFailure(StreamChunkId streamChunkId, String error) {
		this.streamChunkId = streamChunkId;
		this.error = error;
	}

	@Override
	public Type type() {
		return Type.ChunkFetchFailure;
	}

	@Override
	public int encodedLength() {
		return streamChunkId.encodedLength() + CodecUtil.Strings.encodedLength(error);
	}

	@Override
	public void encode(ByteBuf buf) {
		streamChunkId.encode(buf);
		CodecUtil.Strings.encode(buf, error);
	}

	public static ChunkFetchFailure decode(ByteBuf buf) {
		StreamChunkId streamChunkId = StreamChunkId.decode(buf);
		String error = Strings.decode(buf);
		return new ChunkFetchFailure(streamChunkId, error);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ChunkFetchFailure) {
			ChunkFetchFailure o = (ChunkFetchFailure) obj;
			return streamChunkId.equals(o.streamChunkId) &&
					error.equals(o.error);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(this);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamChunkId", streamChunkId)
				.add("error", error)
				.toString();
	}
}
