package govind.incubator.network.protocol;

import com.google.common.base.Objects;
import govind.incubator.network.util.CodecUtil;
import io.netty.buffer.ByteBuf;

public class StreamFailure extends AbstractMessage implements ResponseMessage {
	final String streamId;
	final String error;

	public StreamFailure(String streamId, String error) {
		this.streamId = streamId;
		this.error = error;
	}

	@Override
	public Type type() {
		return Type.StreamFailure;
	}

	@Override
	public int encodedLength() {
		return CodecUtil.Strings.encodedLength(streamId) +
				CodecUtil.Strings.encodedLength(error);
	}

	@Override
	public void encode(ByteBuf buf) {
		CodecUtil.Strings.encode(buf, streamId);
		CodecUtil.Strings.encode(buf, error);
	}

	public static StreamFailure decode(ByteBuf buf) {
		String streamId = CodecUtil.Strings.decode(buf);
		String error = CodecUtil.Strings.decode(buf);
		return new StreamFailure(streamId, error);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(streamId, error);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StreamFailure) {
			StreamFailure o = (StreamFailure) obj;
			return streamId.equals(o.streamId) &&
					error.equals(o.error);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("streamId", streamId)
				.add("error", error)
				.toString();
	}
}
