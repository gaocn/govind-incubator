package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.buffer.ManagedBuffer;
import govind.incubator.network.buffer.NettyManagedBuffer;
import io.netty.buffer.ByteBuf;

/**
 * 服务端返回的编码数据并不包括在该消息中，为了充分利用Netty特性(例如：Zero-Copy)
 * 进行更加高效的数据传输！
 */
public class ChunkFetchSuccess extends AbstractResponseMessage {
	public final StreamChunkId streamChunkId;

	public ChunkFetchSuccess(ManagedBuffer body, StreamChunkId streamChunkId) {
		super(body, true);
		this.streamChunkId = streamChunkId;
	}

	@Override
	public ResponseMessage createFailureResponse(String error) {
		return new ChunkFetchFailure(streamChunkId, error);
	}

	@Override
	public Type type() {
		return Type.ChunkFetchSuccess;
	}

	@Override
	public int encodedLength() {
		return streamChunkId.encodedLength();
	}

	@Override
	public void encode(ByteBuf buf) {
		streamChunkId.encode(buf);
	}

	public static ChunkFetchSuccess decode(ByteBuf buf) {
		StreamChunkId streamChunkId = StreamChunkId.decode(buf);
		buf.retain();
		NettyManagedBuffer managedBuffer = new NettyManagedBuffer(buf);
		return new ChunkFetchSuccess(managedBuffer, streamChunkId);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamChunkId, body());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ChunkFetchSuccess) {
			ChunkFetchSuccess o = (ChunkFetchSuccess) obj;
			return streamChunkId.equals(o.streamChunkId) &&
					super.equals(o);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamChunkId", streamChunkId)
				.add("body", body())
				.toString();
	}
}
